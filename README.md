# pulsar-io-huawei-function-graph
Pulsar IO connector for huawei FunctionGraph https://www.huaweicloud.com/en-us/product/functiongraph.html

## Configuration
The configuration of the FunctionGraph sink connector has the following properties.

### Property

| Name | Type|Required | Default | Description
|------|----------|----------|---------|-------------|
| `region` |String|true|" " (empty string) | Information about the region where the FunctionGraph is located. |
| `functionGraphEndpoint` |String|true|" " (empty string) | Domain name or IP address of the server bearing the FunctionGraph REST service. |
| `projectId` | String|true|" " (empty string) | User Project ID. |
| `ak` |String| true|" " (empty string) | User AK. |
| `sk` | String|true|" " (empty string) | User SK. |
| `functionUrn` | String|true|" " (empty string) | Function URN. |

### Example

Before using the FunctionGraph sink connector, you need to create a configuration file through one of the following methods.

* JSON

    ```json
    {
        "region": "cn-north-4",
        "functionGraphEndpoint": "https://functiongraph.cn-north-4.myhuaweicloud.com",
        "projectId": "fakeProjectId",
        "ak": "fakeSK",
        "sk": "fakeSK",
        "functionUrn": "example function urn"
    }
    ```

* YAML

    ```yaml
    configs:
        region: "cn-north-4"
        functionGraphEndpoint: "https://functiongraph.cn-north-4.myhuaweicloud.com"
        projectId: "fakeProjectId"
        ak: "fakeAK"
        sk: "fakeSK"
        functionUrn: "example function urn"
    ```

