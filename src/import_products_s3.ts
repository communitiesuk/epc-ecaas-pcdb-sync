import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { saveProducts } from "./save_products.js";
import type { BreResponse } from "./pcdb.types.js";

const readFile = async (fileName: string) => {
	console.log(`Reading data from ${fileName}`);

    const params = {
		Bucket: "epb-ecaas-pcdb",
		Key: fileName,
	};

	const command = new GetObjectCommand(params);
	const s3Client = new S3Client({});
	let s3Response;

	try {
		s3Response = await s3Client.send(command);
	} catch (error) {
		console.error("Error reading data from S3", error);
		return;
	}

	return await s3Response.Body?.transformToString();
};

export const importProducts = async (fileName: string) => {
    const data = await readFile(fileName);
	const response = data ? JSON.parse(data) as BreResponse : undefined;

    await saveProducts(response);
};