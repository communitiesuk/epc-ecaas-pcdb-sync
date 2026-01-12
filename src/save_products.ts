import { batchItems } from "./utils/batch_items.js";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { BatchWriteCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { type BreResponse, type BreProduct, type ProductData } from "./pcdb.types.js";
import { keysToCamelCase } from "./utils/objects.js";

const localDynamoDBConfig = {
	region: "fakeRegion", 
	endpoint: "http://localhost:8000",
	credentials: {
		accessKeyId: "fakeMyKeyId",
		secretAccessKey: "fakeSecretAccessKey",
	}
};

const client = new DynamoDBClient(process.env.NODE_ENV === "development" ? localDynamoDBConfig : {});
const docClient = DynamoDBDocumentClient.from(client);

export const saveProducts = async (response: BreResponse | undefined) => {
	console.log("Save products");

	if (!response) {
		console.log('No products to save');
		return;
	}

	for (const productType of response.productTypes) {
		try {
			await saveProductType(productType);
		}
		catch (err: unknown) {
			console.error(`Error writing ${productType.productTypeName} data to DynamoDB`, err);
			break;
		}
	}
}

const saveProductType = async (productsResponse: BreProduct) => {
	const products = (productsResponse?.data ?? []) as Record<string, unknown>[];

	const batchedProducts = batchItems(products);
	let completedBatches = 0;

	if (!batchedProducts.length) {
		return;
	}

	console.log(`Saving ${batchedProducts.length} batches to DynamoDB`);

	for (const batch of batchedProducts) {
		if (!batch?.length) {
			continue;
		}

		await docClient.send(
			new BatchWriteCommand({
				RequestItems: {
					"products": batch.map(x => {
						const data = keysToCamelCase(x);

						const item: ProductData = {
							...data,
							id: (data.id ?? data.productID) as string,
							brandName: (data.brandName ?? "") as string,
							modelName: (data.modelName ?? "") as string,
							technologyType: productsResponse.productType.trim(),
						};

						return {
							PutRequest: {
								Item: {
									...item,
									testData: Array.isArray(data.testData) ? data.testData.map(td => keysToCamelCase(td)) : [],
									"sk-by-brand": `${item.brandName.toLowerCase()}#${item.modelName.toLowerCase()}#${item.modelQualifier?.toLowerCase() ?? ""}`,
									"sk-by-model": `${item.modelName.toLowerCase()}#${item.modelQualifier?.toLowerCase() ?? ""}`,
								}
							}
						};
					}),
				},
			}),
		);

		completedBatches++;
	}

	console.log(`Completed ${completedBatches} of ${batchedProducts.length} batches`);
};