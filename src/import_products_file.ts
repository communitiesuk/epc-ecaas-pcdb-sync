import type { BreResponse } from "./pcdb.types.js";
import { saveProducts } from "./save_products.js";
import { readFile } from "fs/promises";

export const importProducts = async (filePath: string) => {
	const file = await readFile(filePath, 'utf-8');
	const response = JSON.parse(file) as BreResponse;

	await saveProducts(response);
}