CREATE TABLE `mydbname`.`d_customer` (
  `customer_id` INT NOT NULL,
  `customer_fn` VARCHAR(100) NULL,
  `customer_ln` VARCHAR(200) NULL,
  `customer_street` VARCHAR(255) NULL,
  `customer_state` VARCHAR(100) NULL,
  `customer_zcode` VARCHAR(45) NULL,
  PRIMARY KEY (`customer_id`));

CREATE TABLE `mydbname`.`d_products` (
  `product_id` INT NOT NULL,
  `product_name` VARCHAR(150) NULL,
  PRIMARY KEY (`product_id`));

CREATE TABLE `f_purchase` (
  `purchase_id` int(11) NOT NULL AUTO_INCREMENT,
  `customer_id` int(11) DEFAULT NULL,
  `product_id` int(11) DEFAULT NULL,
  `purchase_status` varchar(45) DEFAULT NULL,
  `purchase_date` varchar(45) DEFAULT NULL,
  `purchase_amount` double DEFAULT NULL,
  `alert` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`purchase_id`)
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=latin1;

CREATE TABLE `sys_ddl` (
`idsys_ddl` int(11) NOT NULL AUTO_INCREMENT,
`ddl_name` varchar(100) DEFAULT NULL,
`ddl_create` longtext DEFAULT NULL,
`ddl_drop` longtext DEFAULT NULL,
PRIMARY KEY (`idsys_ddl`)
)

CREATE TABLE `sys_triggers` (
  `sp_name` varchar(45) DEFAULT NULL,
  `sp_switch` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;




INSERT INTO `mydbname`.`sys_ddl`
(`idsys_ddl`,
`ddl_name`,
`ddl_create`,
`ddl_drop`)
VALUES
(0,
"create_d_customer_stg",
"CREATE TABLE `mydbname`.`d_customer_stg` (
  `customer_id` INT NOT NULL,
  `customer_fn` VARCHAR(100) NULL,
  `customer_ln` VARCHAR(200) NULL,
  `customer_street` VARCHAR(255) NULL,
  `customer_state` VARCHAR(100) NULL,
  `customer_zcode` VARCHAR(45) NULL,
  PRIMARY KEY (`customer_id`))",
"DROP TABLE IF EXISTS `mydbname`.`d_customer_stg`");

INSERT INTO `mydbname`.`sys_ddl`
(`idsys_ddl`,
`ddl_name`,
`ddl_create`,
`ddl_drop`)
VALUES
(0,
"create_d_porducts_stg",
"CREATE TABLE `mydbname`.`d_products_stg` (
  `product_id` INT NOT NULL,
  `product_name` VARCHAR(150) NULL,
  PRIMARY KEY (`product_id`))",
"DROP TABLE IF EXISTS `mydbname`.`d_products_stg`");

INSERT INTO `mydbname`.`sys_ddl`
(`idsys_ddl`,
`ddl_name`,
`ddl_create`,
`ddl_drop`)
VALUES
(0,
"create_f_purchase_stg",
"CREATE TABLE `f_purchase_stg` (
  `purchase_id` int(11) NOT NULL DEFAULT 0,
  `customer_id` int(11) DEFAULT NULL,
  `product_id` int(11) DEFAULT NULL,
  `purchase_status` varchar(45) DEFAULT NULL,
  `purchase_date` varchar(45) DEFAULT NULL,
  `purchase_amount` double DEFAULT NULL)",
"DROP TABLE IF EXISTS `mydbname`.`f_purchase_stg`");

INSERT INTO `mydbname`.`sys_ddl`
(`idsys_ddl`,
`ddl_name`,
`ddl_create`,
`ddl_drop`)
VALUES
(0,
"update_tables",
"CALL `mydbname`.`update_tables`()",
"select 1");




DELIMITER $$
CREATE DEFINER=`wordpress`@`%` PROCEDURE `update_tables`()
BEGIN
#Update d_customer
UPDATE mydbname.d_customer c, mydbname.d_customer_stg cs
SET 
c.customer_street = cs.customer_street,
c.customer_state = cs.customer_state,
c.customer_zcode = cs.customer_zcode
WHERE c.customer_id = cs.customer_id;

INSERT INTO mydbname.d_customer(customer_id, customer_fn, customer_ln, customer_street, customer_state, customer_zcode) 
SELECT cs.customer_id, cs.customer_fn, cs.customer_ln, cs.customer_street, cs.customer_state, cs.customer_zcode
FROM mydbname.d_customer_stg cs left outer join mydbname.d_customer c ON cs.customer_id = c.customer_id
WHERE c.customer_id is null;

#Update d_products
UPDATE mydbname.d_products c, mydbname.d_products_stg cs
SET 
c.product_name = cs.product_name
WHERE c.product_id = cs.product_id;

INSERT INTO mydbname.d_products(product_id, product_name) 
SELECT cs.product_id, cs.product_name
FROM mydbname.d_products_stg cs left outer join mydbname.d_products c ON cs.product_id = c.product_id
WHERE c.product_id is null;

#New purchases
INSERT INTO f_purchase(purchase_id, customer_id, product_id, purchase_status, purchase_date, purchase_amount)
SELECT 0, ps.customer_id, ps.product_id, ps.purchase_status, ps.purchase_date, ps.purchase_amount
FROM f_purchase_stg ps left join f_purchase p ON ps.customer_id = p.customer_id and ps.product_id = p.product_id
WHERE ps.purchase_status = 'new' and p.purchase_id is null;

#Cancel purchase with previous New
INSERT INTO f_purchase(purchase_id, customer_id, product_id, purchase_status, purchase_date, purchase_amount)
SELECT 0, ps.customer_id, ps.product_id, ps.purchase_status, ps.purchase_date, ps.purchase_amount
FROM f_purchase_stg ps inner join f_purchase p ON ps.customer_id = p.customer_id and ps.product_id = p.product_id
INNER JOIN 
	(SELECT ps.customer_id, ps.product_id, count(distinct p.purchase_status) total_purchases
	FROM f_purchase_stg ps inner join f_purchase p ON ps.customer_id = p.customer_id and ps.product_id = p.product_id
	group by ps.customer_id, ps.product_id) A ON ps.customer_id = A.customer_id and ps.product_id = A.product_id
WHERE ps.purchase_status = 'canceled' and p.purchase_status = 'new' and (A.total_purchases = 1);

#Cancel purchase without previous New
INSERT INTO f_purchase(purchase_id, customer_id, product_id, purchase_status, purchase_date, purchase_amount, alert)
SELECT 0, ps.customer_id, ps.product_id, ps.purchase_status, ps.purchase_date, ps.purchase_amount, IFNULL(p.purchase_id, "No previous new order found") alert
FROM f_purchase_stg ps left join f_purchase p ON ps.customer_id = p.customer_id and ps.product_id = p.product_id
WHERE ps.purchase_status = 'canceled' and p.purchase_id is null;

END$$
DELIMITER ;



DELIMITER $$
CREATE TRIGGER `start_update_tables_sp` AFTER UPDATE ON `sys_triggers`
FOR EACH ROW
BEGIN
CALL `mydbname`.`update_tables`();
END;
$$


INSERT INTO `mydbname`.`sys_triggers`
(`sp_name`,
`sp_switch`)
VALUES
("update_tables",
0);
