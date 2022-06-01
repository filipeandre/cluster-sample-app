let ddbClient = undefined;

function init(awsFactory) {
    ddbClient = new awsFactory.DynamoDB.DocumentClient({apiVersion: '2012-08-10'});
}

async function saveNodeData(nodeId, ipv4Addrs, mainPageHitCounter, healthCheckHitCounter) {
    const params = {
        TableName: 'CLUSTER_SAMPLE_APP_NODE',
        Item: {
            'NODE_ID' : nodeId.toString(),
            'IP_ADDRS' : JSON.stringify(ipv4Addrs),
            'PAGE_HIT_COUNT': mainPageHitCounter.toString(),
            'HEALTHCHECK_HIT_COUNT': healthCheckHitCounter.toString(),
        }
    };

    try {
        await ddbClient.put(params).promise();
    }
    catch(error) {
        console.error("Unable to insert application data: ", error);
    }
}

async function cleanUpNodesData(nodeId) {
    const params = {
        TableName: 'CLUSTER_SAMPLE_APP_NODE',
        Key: {
            'NODE_ID' : nodeId.toString()
        }
    };

    try {
        await ddbClient.delete(params).promise();
    }
    catch(error) {
        console.error("Unable to delete application node data: ", error);
    }
}

async function getAllNodesData(nodeId) {
    const params = {
        ExpressionAttributeValues: {
            ':nodeid': {S: '1'}
        },
        FilterExpression: "NODE_ID <> :nodeid",
        TableName: 'CLUSTER_SAMPLE_APP_NODE'
        };
    
    try {
        const res = await ddbClient.scan(params).promise();
        return res.Items;        
    }
    catch(error) {
        console.error("Unable to get all node data: ", error);
        throw error;
    }
}

async function updateNodeData(nodeId, mainPageHitCounter, healthCheckHitCounter) {
    const params = {
        TableName: 'CLUSTER_SAMPLE_APP_NODE',
        Key: { 'NODE_ID' : nodeId.toString() },
        UpdateExpression: 'set PAGE_HIT_COUNT = :pageHit, HEALTHCHECK_HIT_COUNT = :healthHit',
        ExpressionAttributeValues: {
            ':pageHit' : mainPageHitCounter.toString(),
            ':healthHit' : healthCheckHitCounter.toString()
        }
    };

    try {
        await ddbClient.update(params).promise();
    }
    catch(error) {
        console.error("Unable to update node data: ", error);
        throw error;
    }
}
exports.init = init
exports.saveNodeData = saveNodeData
exports.updateNodeData = updateNodeData
exports.cleanUpNodesData = cleanUpNodesData
exports.getAllNodesData = getAllNodesData
