create database project_db;
use  project_db;
---------------------------------------------------------------------------------------
create table nashville_housing(
  UniqueID varchar(100), ParcelID varchar(100),	LandUse varchar(100),
  PropertyAddress varchar(100),	SaleDate varchar(100),	SalePrice int,
  LegalReference varchar(100),	SoldAsVacant varchar(100), OwnerName varchar(100),
  OwnerAddress varchar(100), Acreage varchar(100),	TaxDistrict varchar(100),
  LandValue int, BuildingValue int,	TotalValue int,	YearBuilt int,
  Bedrooms int,	FullBath int, HalfBath int);
  
  LOAD DATA LOCAL INFILE 'C:\\Users\\admin\\Desktop\\sql\\Data cleaning project\\Nashville Housing Data for Data Cleaning.csv'
  INTO TABLE nashville_housing FIELDS TERMINATED BY ','
  ENCLOSED BY ' " '
  LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
 ---------------------------------------------------------------------------------------
 select * from nashville_housing;
 
 ---------------------------------------------------------------------------------------
 /* Cleaning Data in SQL Queries */
 
select * from nashville_housing;
------------------------------------------------
-- Standardize date format

select SaleDate
from nashville_housing;

ALTER TABLE nashville_housing
Add column SaleDateConverted Date;

SET SQL_SAFE_UPDATES = 0;

UPDATE nashville_housing
SET SaleDateConverted = str_to_date(SaleDate, '%M %d, %Y');

select SaleDate, SaleDateConverted
from nashville_housing;

SET SQL_SAFE_UPDATES = 1;
----------------------------------------------------------------------------------------------
-- Populate Property Address data

SELECT PropertyAddress
FROM nashville_housing
WHERE PropertyAddress = '';

SELECT *
FROM nashville_housing
-- WHERE PropertyAddress = ''
order by ParcelID;

select a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress
from nashville_housing a
join nashville_housing b
     on a.ParcelID = b.ParcelID
     and a.UniqueID <> b.UniqueID
where a.PropertyAddress = '';

SELECT 
    a.ParcelID, 
    a.PropertyAddress, 
    b.ParcelID, 
    b.PropertyAddress, 
    COALESCE(b.PropertyAddress, a.PropertyAddress)
FROM 
    nashville_housing a
JOIN 
    nashville_housing b
ON 
    a.ParcelID = b.ParcelID
    AND a.UniqueID <> b.UniqueID
WHERE 
    a.PropertyAddress = '';

SET SQL_SAFE_UPDATES = 0;


UPDATE nashville_housing a
JOIN nashville_housing b
ON a.ParcelID = b.ParcelID
AND a.UniqueID <> b.UniqueID
SET a.PropertyAddress = COALESCE(b.PropertyAddress, a.PropertyAddress)
WHERE a.PropertyAddress = '';
    
SET SQL_SAFE_UPDATES = 1;
------------------------------------------------------------------------------------------
-- Breaking out Address into Individual Columns (Address, City, State)

SELECT PropertyAddress
FROM nashville_housing;

SELECT 
substring(PropertyAddress, 1, instr(',', PropertyAddress)) AS Address
FROM nashville_housing;

SELECT 
    SUBSTRING(PropertyAddress, 1, INSTR(PropertyAddress, ',')) AS Address,
    INSTR(PropertyAddress, ',')
FROM nashville_housing;
    
SELECT 
    SUBSTRING(PropertyAddress, 1, INSTR(PropertyAddress, ',') -1) AS Address,
    SUBSTRING(PropertyAddress, INSTR(PropertyAddress, ',') +1 , LENGTH(PropertyAddress)) as Address
FROM nashville_housing;

ALTER TABLE nashville_housing
Add column PropertySplitAddress varchar(255);

SET SQL_SAFE_UPDATES = 0;

Update nashville_housing
SET PropertySplitAddress = SUBSTRING(PropertyAddress, 1, INSTR(PropertyAddress, ',') -1);

ALTER TABLE nashville_housing
Add column PropertySplitCity varchar(255);

Update nashville_housing
SET PropertySplitCity = SUBSTRING(PropertyAddress, INSTR(PropertyAddress, ',') +1 , LENGTH(PropertyAddress));

SET SQL_SAFE_UPDATES = 1;

SELECT OWNERADDRESS
FROM nashville_housing;

SELECT SUBSTRING_INDEX(REPLACE(OwnerAddress, ',', '.'), '.', -1) AS Part_1,
       SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(OwnerAddress, ',', '.'), '.', -2),'.', 1) AS Part_2,
       SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(OwnerAddress, ',', '.'), '.', -3), '.', 1) AS Part_3
FROM nashville_housing;

SET SQL_SAFE_UPDATES = 0;

ALTER TABLE nashville_housing
Add OwnerSplitAddress varchar(255);

Update nashville_housing
SET OwnerSplitAddress = SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(OwnerAddress, ',', '.'), '.', -3), '.', 1);


ALTER TABLE nashville_housing
Add OwnerSplitCity varchar(255);

Update nashville_housing
SET OwnerSplitCity = SUBSTRING_INDEX(SUBSTRING_INDEX(REPLACE(OwnerAddress, ',', '.'), '.', -2),'.', 1);



ALTER TABLE nashville_housing
Add OwnerSplitState varchar(255);

Update nashville_housing
SET OwnerSplitState = SUBSTRING_INDEX(REPLACE(OwnerAddress, ',', '.'), '.', -1);

SET SQL_SAFE_UPDATES = 1;


SELECT *
FROM nashville_housing;

------------------------------------------------------------------------------------------------------------------
-- Change Y and N to Yes and No in "Sold as Vacant" field

SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant)
FROM nashville_housing
GROUP BY SoldAsVacant
ORDER BY 2;

Select SoldAsVacant
, CASE When SoldAsVacant = 'Y' THEN 'Yes'
	   When SoldAsVacant = 'N' THEN 'No'
	   ELSE SoldAsVacant
	   END
From nashville_housing;


Update nashville_housing
SET SoldAsVacant = CASE When SoldAsVacant = 'Y' THEN 'Yes'
	   When SoldAsVacant = 'N' THEN 'No'
	   ELSE SoldAsVacant
	   END;

Select SoldAsVacant
From nashville_housing;

--------------------------------------------------------------------------------------------------------------------
-- Remove Duplicates

WITH RowNumCTE AS(
Select *,
	ROW_NUMBER() OVER (
	PARTITION BY ParcelID,
				 PropertyAddress,
				 SalePrice,
				 SaleDate,
				 LegalReference
				 ORDER BY
					UniqueID
					) as row_num

From nashville_housing
 -- order by ParcelID
);

WITH RowNumCTE AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference
            ORDER BY UniqueID
        ) AS row_num
    FROM nashville_housing
)
SELECT *
FROM RowNumCTE
WHERE row_num > 1
ORDER BY PropertyAddress;

WITH RowNumCTE AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference
            ORDER BY UniqueID
        ) AS row_num
    FROM nashville_housing
)
DELETE
FROM RowNumCTE
WHERE row_num > 1;
-- ORDER BY PropertyAddress

WITH RowNumCTE AS (
    SELECT UniqueID,
           ROW_NUMBER() OVER (
               PARTITION BY ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference
               ORDER BY UniqueID
           ) AS row_num
    FROM nashville_housing
)
DELETE FROM nashville_housing
WHERE UniqueID IN (
    SELECT UniqueID
    FROM RowNumCTE
    WHERE row_num > 1
);



Select *
From nashville_housing;

-------------------------------------------------------------------------------------------------------------
-- Delete Unused Columns

Select *
From nashville_housing;


ALTER TABLE nashville_housing
DROP COLUMN OwnerAddress,
DROP COLUMN TaxDistrict,
DROP COLUMN PropertyAddress,
DROP COLUMN SaleDate;

