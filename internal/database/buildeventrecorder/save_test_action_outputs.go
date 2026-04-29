package buildeventrecorder

import (
	"context"
	"strings"

	bes "github.com/bazelbuild/bazel/src/main/java/com/google/devtools/build/lib/buildeventstream/proto"
	"github.com/buildbarn/bb-portal/ent/gen/ent"
	"github.com/buildbarn/bb-portal/ent/gen/ent/bazelinvocation"
	"github.com/buildbarn/bb-portal/ent/gen/ent/invocationtarget"
	"github.com/buildbarn/bb-portal/ent/gen/ent/testresult"
	"github.com/buildbarn/bb-portal/ent/gen/ent/testsummary"
	"github.com/buildbarn/bb-portal/internal/database"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// testResultNaturalKey identifies a TestResult uniquely within an invocation
// by the BEP-provided natural key fields.
type testResultNaturalKey struct {
	label    string
	configID string
	run      int32
	shard    int32
	attempt  int32
}

func keyFromBesTestResult(id *bes.BuildEventId_TestResultId) testResultNaturalKey {
	return testResultNaturalKey{
		label:    id.GetLabel(),
		configID: id.GetConfiguration().GetId(),
		run:      id.GetRun(),
		shard:    id.GetShard(),
		attempt:  id.GetAttempt(),
	}
}

// saveTestActionOutputs persists the per-test files referenced in
// TestResult.test_action_output (test.log, test.xml, per-attempt logs, etc.)
// into the invocation_files table, linked back to the corresponding TestResult
// via the new test_action_outputs edge.
//
// Files without a digest are skipped (we couldn't fetch them from CAS anyway).
// The bulk insert uses ON CONFLICT DO NOTHING so replayed BES events don't
// abort the entire batch on the (name, bazel_invocation) unique constraint.
func saveTestActionOutputs(ctx context.Context, invocationDbID int64, tx database.Handle, batch []BuildEventWithInfo) error {
	if len(batch) == 0 {
		return nil
	}

	// Pre-pass: collect natural keys for the test results that have any
	// action outputs worth persisting. Skip events whose action_output list
	// has no files with digests.
	type pendingResult struct {
		key   testResultNaturalKey
		files []*bes.File
	}
	pending := make([]pendingResult, 0, len(batch))
	for _, x := range batch {
		tr := x.Event.GetTestResult()
		id := x.Event.GetId().GetTestResult()
		if tr == nil || id == nil {
			continue
		}
		var files []*bes.File
		for _, f := range tr.GetTestActionOutput() {
			if getFileDigestFromBesFile(f) == nil {
				continue
			}
			files = append(files, f)
		}
		if len(files) == 0 {
			continue
		}
		pending = append(pending, pendingResult{
			key:   keyFromBesTestResult(id),
			files: files,
		})
	}
	if len(pending) == 0 {
		return nil
	}

	// Re-query the just-inserted test_results to map natural key -> id.
	// All test results for this invocation share the chain
	// test_summary -> invocation_target -> bazel_invocation, so scope the
	// query by invocation to keep it cheap and indexed.
	results, err := tx.Ent().TestResult.Query().
		Where(
			testresult.HasTestSummaryWith(
				testsummary.HasInvocationTargetWith(
					invocationtarget.HasBazelInvocationWith(
						bazelinvocation.IDEQ(invocationDbID),
					),
				),
			),
		).
		WithTestSummary(func(q *ent.TestSummaryQuery) {
			q.WithInvocationTarget(func(q *ent.InvocationTargetQuery) {
				q.WithTarget()
				q.WithConfiguration()
			})
		}).
		All(ctx)
	if err != nil {
		return util.StatusWrap(err, "Failed to look up test results for action output linking")
	}

	idByKey := make(map[testResultNaturalKey]int64, len(results))
	for _, tr := range results {
		ts := tr.Edges.TestSummary
		if ts == nil || ts.Edges.InvocationTarget == nil {
			continue
		}
		it := ts.Edges.InvocationTarget
		if it.Edges.Target == nil {
			continue
		}
		configID := ""
		if it.Edges.Configuration != nil {
			configID = it.Edges.Configuration.ConfigurationID
		}
		idByKey[testResultNaturalKey{
			label:    it.Edges.Target.Label,
			configID: configID,
			run:      tr.Run,
			shard:    tr.Shard,
			attempt:  tr.Attempt,
		}] = tr.ID
	}

	// Build the bulk insert. One InvocationFiles row per (test_result, file).
	builders := make([]*ent.InvocationFilesCreate, 0)
	for _, p := range pending {
		testResultID, ok := idByKey[p.key]
		if !ok {
			// Should not happen if createTestResultsBulk succeeded, but
			// don't crash the batch if the natural-key lookup misses.
			continue
		}
		for _, f := range p.files {
			pathPrefix := append([]string{}, f.GetPathPrefix()...)
			pathPrefix = append(pathPrefix, f.GetName())
			fullName := strings.Join(pathPrefix, "/")
			if fullName == "" {
				continue
			}

			c := tx.Ent().InvocationFiles.Create().
				SetName(fullName).
				SetBazelInvocationID(invocationDbID).
				SetTestResultID(testResultID)

			if digest := getFileDigestFromBesFile(f); digest != nil {
				c.SetDigest(*digest)
			}
			if length := getFileSizeBytesFromBesFile(f); length != nil {
				c.SetSizeBytes(*length)
			}
			if digestFunction := getFileDigestFunctionFromBesFile(f); digestFunction != nil {
				c.SetDigestFunction(*digestFunction)
			}

			builders = append(builders, c)
		}
	}

	if len(builders) == 0 {
		return nil
	}

	// DoNothing on the (name, bazel_invocation) unique constraint so replayed
	// BES events don't abort the transaction.
	if err := tx.Ent().InvocationFiles.CreateBulk(builders...).OnConflict().DoNothing().Exec(ctx); err != nil {
		return util.StatusWrap(err, "Failed to bulk insert test action output files")
	}
	return nil
}
