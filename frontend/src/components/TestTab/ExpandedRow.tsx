import { FileTextOutlined } from "@ant-design/icons";
import { List, Space, Typography } from "antd";
import type React from "react";
import { digestFunctionValueFromString } from "@/utils/digestFunctionUtils";
import { readableFileSize } from "@/utils/filesize";
import { generateFileUrl } from "@/utils/urlGenerator";
import TestStatusTag, { type TestStatusEnum } from "../TestStatusTag";
import type { TestTabRowType } from "./columns";

type TestResult = NonNullable<TestTabRowType["testResults"]>[number];
type ActionOutput = NonNullable<TestResult["actionOutputs"]>[number];

const basename = (path: string): string => path.split("/").pop() ?? path;

const buildOutputUrl = (
  instanceName: string,
  ao: ActionOutput,
): string =>
  generateFileUrl(
    instanceName,
    digestFunctionValueFromString(ao.digestFunction),
    {
      hash: ao.digest,
      sizeBytes: ao.sizeInBytes.toString(),
    },
    basename(ao.name),
  );

const compareTestResults = (a: TestResult, b: TestResult): number =>
  a.run - b.run || a.shard - b.shard || a.attempt - b.attempt;

interface Props {
  testResults: TestResult[];
  instanceName: string;
}

const ExpandedRow: React.FC<Props> = ({ testResults, instanceName }) => {
  const sorted = [...testResults]
    .filter((tr) => (tr.actionOutputs ?? []).length > 0)
    .sort(compareTestResults);

  if (sorted.length === 0) {
    return (
      <Typography.Text type="secondary">
        No test logs available for this test (cached without outputs, or
        test artifacts have been evicted from CAS).
      </Typography.Text>
    );
  }

  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      {sorted.map((tr) => (
        <div key={tr.id}>
          <Space size="small">
            <TestStatusTag
              displayText={true}
              status={(tr.status ?? "NO_STATUS") as TestStatusEnum}
            />
            <Typography.Text type="secondary">
              run {tr.run} · shard {tr.shard} · attempt {tr.attempt}
            </Typography.Text>
          </Space>
          <List
            size="small"
            dataSource={tr.actionOutputs ?? []}
            renderItem={(ao) => (
              <List.Item>
                <Space>
                  <FileTextOutlined />
                  <a
                    href={buildOutputUrl(instanceName, ao)}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {basename(ao.name)}
                  </a>
                  <Typography.Text type="secondary">
                    ({readableFileSize(ao.sizeInBytes)})
                  </Typography.Text>
                </Space>
              </List.Item>
            )}
          />
        </div>
      ))}
    </Space>
  );
};

export default ExpandedRow;
