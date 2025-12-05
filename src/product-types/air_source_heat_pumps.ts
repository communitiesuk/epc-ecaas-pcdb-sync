import { batchItems } from "../utils/batch_items.js";
import { BatchWriteItemCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import type { BreAirSourceHeatPumpData, BreAirSourceHeatPumpTestData, BreResponse } from "../pcdb.types.ts";

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

export const importAirSourceHeatPumps = async (data: BreResponse[]) => {
	console.log("Import air source heat pumps");

	const productsResponse = data.find(x => x.productType === "Air source heat pumps");
	const products = (productsResponse?.data ?? []) as BreAirSourceHeatPumpData[];
	const testData = (productsResponse?.TestData ?? []) as BreAirSourceHeatPumpTestData[];

	const batchedProducts = batchItems(products);
	let completedBatches = 0;

	console.log(`Saving ${batchedProducts.length} batches to DynamoDB`);

	for (let index = 0; index < batchedProducts.length; index++) {
		const batch = batchedProducts[index];

		if (!batch?.length) {
			continue;
		}

		try {
			await docClient.send(
				new BatchWriteItemCommand({
					RequestItems: {
						"products": batch.map(x => {
							const productTestData = testData.filter(td => td.productID === x.ID);

							return {
								PutRequest: {
									Item: {
										"id": { "N": `${x.ID}` },
										"technologyType": { "S": productsResponse!.productType },
										"brandName": { "S": x.brand },
										"modelName": { "S": x.modelName },
										...x.modelQualifier && { "modelQualifier": { "S": x.modelQualifier } },
										"fuel": { "S": x.energySupply },
										"sourceType": { "S": x.sourceType },
										"sinkType": { "S": x.sinkType },
										"backupControlType": { "S": x.backupCtrlType },
										"modulatingControl": { "BOOL": !!x.modulatingControl },
										...x.standardRatingCapacity20C && { "standardRatingCapacity20C": { "N": `${x.standardRatingCapacity20C}` } },
										...x.standardRatingCapacity35C && { "standardRatingCapacity35C": { "N": `${x.standardRatingCapacity35C}` } },
										...x.standardRatingCapacity55C && { "standardRatingCapacity55C": { "N": `${x.standardRatingCapacity55C}` } },
										...x.minimum_modulation_rate_20 && { "minimumModulationRate20": { "N": `${x.minimum_modulation_rate_20}` } },
										...x.minimum_modulation_rate_35 && { "minimumModulationRate35": { "N": `${x.minimum_modulation_rate_35}` } },
										...x.time_constant_onoff_operation && { "timeConstantOnOffOperation": { "N": `${x.time_constant_onoff_operation}` } },
										...x.temp_return_feed_max && { "tempReturnFeedMax": { "N": `${x.temp_return_feed_max}` } },
										...x.temp_lower_operating_limit && { "tempLowerOperatingLimit": { "N": `${x.temp_lower_operating_limit}` } },
										...x.min_temp_diff_flow_return_for_hp_to_operate && { "minTempDiffFlowReturnForHpToOperate": { "N": `${x.min_temp_diff_flow_return_for_hp_to_operate}` } },
										"varFlowTempCtrlDuringTest": { "BOOL": !!x.var_flow_temp_ctrl_during_test },
										...x.power_heating_circ_pump && { "powerHeatingCircPump": { "N": `${x.power_heating_circ_pump}` } },
										...x.power_heating_warm_air_fan && { "powerHeatingWarmAirFan": { "N": `${x.power_heating_warm_air_fan}` } },
										...x.power_source_circ_pump && { "powerSourceCircPump": { "N": `${x.power_source_circ_pump}` } },
										"powerStandby": { "N": `${x.power_standby}` },
										...x.power_crankcase_heater && { "powerCrankcaseHeater": { "N": `${x.power_crankcase_heater}` } },
										...x.power_off && { "powerOff": { "N": `${x.power_off}` } },
										...x.power_max_backup && { "powerMaximumBackup": { "N": `${x.power_max_backup}` } },
										...productTestData && {
											"testData": {
												"L": productTestData.map(td => (
													{
														"M": {
															"designFlowTemperature": { "N": `${td.design_flow_temp}` },
															"testCondition": { "S": td.test_letter },
															"testConditionTemperature": { "N": `${td.temp_test}` },
															"inletTemperature": { "N": `${td.temp_source}` },
															"outletTemperature": { "N": `${td.temp_outlet}` },
															"heatingCapacity": { "N": `${td.capacity}` },
															"coefficientOfPerformance": { "N": `${td.cop}` },
															"degradationCoefficient": { "N": `${td.degradation_coeff}` },
														},
													}
												)),
											},
										},
										"sk-by-brand": { "S": `${x.brand.toLowerCase()}#${x.modelName.toLowerCase()}#${x.modelQualifier?.toLowerCase() ?? ""}` },
										"sk-by-model": { "S": `${x.modelName.toLowerCase()}#${x.modelQualifier?.toLowerCase() ?? ""}` },
									},
								},
							};
						}),
					},
				}),
			);

			completedBatches++;
		} catch (err: unknown) {
			console.error("Error writing data to DynamoDB", err);
			break;
		}
	}

	console.log(`Completed ${completedBatches} batches of ${batchedProducts.length} products`);
};