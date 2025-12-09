import { batchItems } from "./utils/batch_items.js";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { BatchWriteCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { type BreResponse, type ProductData, type ProductTestData } from "./pcdb.types.js";
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

export const saveProducts = async (data: BreResponse[]) => {
	console.log("Save products");

	for (const productType of data) {
		try {
			await saveProductType(productType);
		}
		catch (err: unknown) {
			console.error(`Error writing ${productType.productType} data to DynamoDB`, err);
			break;
		}
	}
}

const saveProductType = async (productsResponse: BreResponse) => {
	const products = (productsResponse?.data ?? []) as ProductData[];
	const testData = (productsResponse?.TestData ?? []) as ProductTestData[];

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
						const productTestData = testData.filter(td => td.productID === data.id);

						data["testData"] = productTestData.map(td => keysToCamelCase(td));

						const item: ProductData = {
							...data,
							id: (data.id ?? data.productID) as string,
							brandName: (data.brand ?? data.brandName ?? "") as string,
							modelName: (data.modelName ?? "") as string,
							technologyType: productsResponse.productType.trim().toLowerCase(),
						};

						return {
							PutRequest: {
								Item: {
									...item,
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