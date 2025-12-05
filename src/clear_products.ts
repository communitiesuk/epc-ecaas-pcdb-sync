import { BatchWriteItemCommand, DynamoDBClient, ScanCommand, type AttributeValue } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { batchItems } from "./utils/batch_items.js";

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

const getProducts = async (lastEvaluationKey?: Record<string, AttributeValue>, products: Record<string, AttributeValue>[] = []): Promise<Record<string, AttributeValue>[]> => {
	const result = await docClient.send(new ScanCommand({
		TableName: "products",
		ExclusiveStartKey: lastEvaluationKey,
	}));

	const items = products.concat(result.Items ?? []);

	if (result.LastEvaluatedKey) {
		return getProducts(result.LastEvaluatedKey, items);
	}

	return items;
};

export const clearProducts = async () => {
	console.log("Clearing products from DynamoDB");

	const products = await getProducts(undefined, []);
	const batchedProducts = batchItems(products);

	for (let index = 0; index < batchedProducts.length; index++) {
		const batch = batchedProducts[index];

		if (!batch?.length) {
			continue;
		}

		try {
			await docClient.send(
				new BatchWriteItemCommand({
					RequestItems: {
						"products": batch.map(x => ({
							DeleteRequest: {
								Key: { "id": x.id! },
							},
						})),
					},
				}),
			);
		} catch (err: unknown) {
			console.error("Failed to clear existing products", err);
		}
	}

	console.log(`Cleared ${products.length} products`);
};