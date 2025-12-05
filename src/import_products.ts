import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { importAirSourceHeatPumps } from "./product-types/air_source_heat_pumps.js";

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
    const products = data ? JSON.parse(data) : [];

    await importAirSourceHeatPumps(products);
};