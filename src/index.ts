import { clearProducts } from "./clear_products.js";
import { importProducts } from "./import_products.js";

export const handler = async () => {
	await clearProducts();
  	await importProducts("export.json");
};