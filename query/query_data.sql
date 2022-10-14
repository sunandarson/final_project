SELECT 	id_transaction,
		bc.id_customer,
		name_customer,
		birthdate_customer,
		gender_customer,
		country_customer,
		date_transaction,
		product,
		product_transaction,
		amount_transaction
FROM bigdata_customer bc 
	LEFT JOIN bigdata_transaction bt  ON bc.id_customer = bt.id_customer 
	LEFT JOIN bigdata_product bp ON bp.type_product = bt.product_transaction  ;