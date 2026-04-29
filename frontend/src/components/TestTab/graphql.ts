import { gql } from "@/graphql/__generated__";

export const GET_TESTS_FOR_INVOCATION = gql(/* GraphQl */ `
  query GetTestsForInvocation(
    $after: Cursor
    $first: Int
    $before: Cursor
    $last: Int
    $orderBy: TestSummaryOrder
    $where: TestSummaryWhereInput
  ) {
    findTestSummaries(
      after: $after
      first: $first
      before: $before
      last: $last
      orderBy: $orderBy
      where: $where
    ) {
      pageInfo {
        startCursor
        endCursor
        hasNextPage
        hasPreviousPage
      }
      edges {
        node {
          id
          overallStatus
          totalRunDurationInMs
          testResults {
            id
            run
            shard
            attempt
            status
            cachedLocally
            cachedRemotely
            actionOutputs {
              name
              digest
              sizeInBytes
              digestFunction
            }
          }
          invocationTarget {
            target {
              instanceName {
                name
              }
              label
              aspect
              targetKind
            }
          }
        }
      }
    }
  }
`);
