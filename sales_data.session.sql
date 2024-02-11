ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN date_uuid SET NOT NULL,

	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN user_uuid SET NOT NULL,

	ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN card_number SET NOT NULL,

	ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN store_code SET NOT NULL,

	ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_code SET NOT NULL,

	ALTER COLUMN product_quantity TYPE SMALLINT,
    ALTER COLUMN product_quantity SET NOT NULL;

-- dim_users--

ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN first_name SET NOT NULL,

	ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN last_name SET NOT NULL,

	ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
    ALTER COLUMN date_of_birth SET NOT NULL,

	ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN country_code SET NOT NULL,

	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN user_uuid SET NOT NULL,

	ALTER COLUMN join_date TYPE DATE USING join_date::DATE,
    ALTER COLUMN join_date SET NOT NULL;

SELECT * FROM dim_store_details WHERE index = 0

-- dim_store_details --

UPDATE dim_store_details
    SET address = NULL,
        longitude = NULL, 
        locality = NULL,
        latitude = NULL,
        country_code = 'GB'
    WHERE index = 0;

ALTER TABLE dim_store_details
ALTER COLUMN country_code DROP NOT NULL;


ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
   
	ALTER COLUMN locality TYPE VARCHAR(255),

	ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN store_code SET NOT NULL,

	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN staff_numbers SET NOT NULL,

	ALTER COLUMN store_type TYPE VARCHAR(255),

	ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN opening_date SET NOT NULL,

	ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,

	ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN country_code SET NOT NULL,

	ALTER COLUMN continent TYPE VARCHAR(255),
    ALTER COLUMN continent SET NOT NULL;


-- PRODUCTS --

ALTER TABLE dim_products
    ADD COLUMN weight_class VARCHAR(14);

UPDATE dim_products
    SET weight_class =
        CASE
            WHEN weight < 2 THEN 'Light'
            WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
            WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
            WHEN weight >= 140 THEN 'Truck_Required'
        END;

ALTER TABLE dim_products
    RENAME COLUMN removed TO still_available;

SELECT LENGTH(time_period) AS len_code FROM dim_date_times ORDER BY len_code DESC

UPDATE dim_products
    SET still_available =
        CASE
            WHEN still_available = 'Still_avaliable' THEN true
            WHEN still_available = 'Removed' THEN false
        END;

SELECT * FROM dim_products

UPDATE dim_products
    SET product_price = REPLACE(product_price, '£', '')
    WHERE product_price LIKE '%£%';

ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN product_price SET NOT NULL,

    ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
    ALTER COLUMN weight SET NOT NULL,

    ALTER COLUMN "EAN" TYPE VARCHAR(17),
    ALTER COLUMN "EAN" SET NOT NULL,

    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_code SET NOT NULL,

    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN date_added SET NOT NULL,

    ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
    ALTER COLUMN uuid SET NOT NULL,

    ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,
    ALTER COLUMN still_available SET NOT NULL,

    ALTER COLUMN weight_class TYPE VARCHAR(14),
    ALTER COLUMN weight_class SET NOT NULL;


SELECT LENGTH(month) AS m ,LENGTH(year) AS y, day FROM dim_date_times

-- dim_date_times--

ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(2),
    ALTER COLUMN month SET  NOT NULL,

    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN day SET NOT NULL,

    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN year SET NOT NULL,

    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN time_period SET NOT NULL,

    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
    ALTER COLUMN date_uuid SET NOT NULL; 

SELECT expiry_date, card_number FROM dim_card_details ORDER BY expiry_date DESC

-- dim_card_details--
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(22),
    ALTER COLUMN card_number SET NOT NULL,

    ALTER COLUMN expiry_date TYPE VARCHAR(5),
    ALTER COLUMN expiry_date SET NOT NULL,

    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE,
    -- USING to_timestamp(date_payment_confirmed)::DATE,
    ALTER COLUMN date_payment_confirmed SET NOT NULL;

SELECT LENGTH(card_number) AS len_card FROM test_card ORDER BY len_card DESC

-- Primary key constraints 

ALTER TABLE dim_users
    ADD CONSTRAINT pk_dim_users PRIMARY KEY (user_uuid);

ALTER TABLE dim_store_details
    ADD CONSTRAINT pk_dim_store_details PRIMARY KEY (store_code);

ALTER TABLE dim_products
    ADD CONSTRAINT pk_dim_products PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
    ADD CONSTRAINT pk_dim_date_times PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
    ADD CONSTRAINT pk_dim_card_details PRIMARY KEY (card_number);


ALTER TABLE test_card
    DROP CONSTRAINT pk_dim_card_details

ALTER TABLE orders_table
    DROP CONSTRAINT fk_card_details_card_number

SELECT card_number FROM test_card
SELECT card_number FROM orders_table 


-- Rows in table1 that do not have matches in table2
SELECT card_number
FROM orders_table
WHERE card_number NOT IN (SELECT card_number FROM test_card);

-- Rows in table2 that do not have matches in table1
SELECT card_number
FROM test_card
WHERE card_number NOT IN (SELECT card_number FROM orders_table);



-- Foreign key constraints--

ALTER TABLE orders_table 
    -- RAN and DONE
    -- ADD CONSTRAINT fk_users_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
    -- RAN and DONE
    -- ADD CONSTRAINT fk_store_details_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
    -- RAN and DONE
    -- ADD CONSTRAINT fk_products_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
    
    -- RAN and DONE
    -- ADD CONSTRAINT fk_date_times_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
    
    ADD CONSTRAINT fk_card_details_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);



