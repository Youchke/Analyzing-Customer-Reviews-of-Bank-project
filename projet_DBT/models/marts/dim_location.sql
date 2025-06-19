{{ config(
    materialized='table',
    unique_key='location_id',
    tags=['dimension']
) }}

WITH extracted_locations AS (
  SELECT
    TRIM(
      SUBSTRING(bank_location FROM '.*,\\s*(.*)$')
    ) AS raw_city,
    bank_location
  FROM {{ source('source_data', 'raw_reviews_brutes') }}
),

cleaned_locations AS (
  SELECT
    CASE
      WHEN raw_city ILIKE '%Rabat%' THEN 'Rabat'
      WHEN raw_city ILIKE '%Casablanca%' OR raw_city ILIKE '%Casa%' THEN 'Casablanca'
      WHEN raw_city ILIKE '%Mohammédia%' OR raw_city ILIKE '%Mohammedia%' THEN 'Mohammédia'
      WHEN raw_city ILIKE '%El Jadida%' OR raw_city ILIKE '%Jadida%' THEN 'El Jadida'
      WHEN raw_city ILIKE '%Settat%' THEN 'Settat'
      WHEN raw_city ILIKE '%Berrechid%' THEN 'Berrechid'
      WHEN raw_city ILIKE '%Marrakech%' OR raw_city ILIKE '%Marrak%' THEN 'Marrakech'
      WHEN raw_city ILIKE '%Safi%' THEN 'Safi'
      WHEN raw_city ILIKE '%Essaouira%' THEN 'Essaouira'
      WHEN raw_city ILIKE '%Fès%' OR raw_city ILIKE '%Fes%' OR raw_city ILIKE '%Fez%' THEN 'Fès'
      WHEN raw_city ILIKE '%Meknès%' OR raw_city ILIKE '%Meknes%' THEN 'Meknès'
      WHEN raw_city ILIKE '%Taza%' THEN 'Taza'
      WHEN raw_city ILIKE '%Tanger%' THEN 'Tanger'
      WHEN raw_city ILIKE '%Tétouan%' OR raw_city ILIKE '%Tetouan%' THEN 'Tétouan'
      WHEN raw_city ILIKE '%Larache%' THEN 'Larache'
      WHEN raw_city ILIKE '%Ksar El Kebir%' THEN 'Ksar El Kebir'
      WHEN raw_city ILIKE '%Agadir%' THEN 'Agadir'
      WHEN raw_city ILIKE '%Taroudant%' THEN 'Taroudant'
      WHEN raw_city ILIKE '%Oujda%' THEN 'Oujda'
      WHEN raw_city ILIKE '%Nador%' THEN 'Nador'
      WHEN raw_city ILIKE '%Kénitra%' OR raw_city ILIKE '%Kenitra%' THEN 'Kénitra'
      WHEN raw_city ILIKE '%Salé%' OR raw_city ILIKE '%Sale%' THEN 'Salé'
      WHEN raw_city ILIKE '%Béni Mellal%' OR raw_city ILIKE '%Beni Mellal%' THEN 'Béni Mellal'
      WHEN raw_city ILIKE '%Khouribga%' THEN 'Khouribga'
      WHEN raw_city ILIKE '%Errachidia%' THEN 'Errachidia'
      WHEN raw_city ILIKE '%Laâyoune%' OR raw_city ILIKE '%Laayoune%' THEN 'Laâyoune'
      WHEN raw_city ILIKE '%Dakhla%' THEN 'Dakhla'
      ELSE raw_city
    END AS city,

    CASE
      WHEN raw_city ILIKE '%Rabat%' OR raw_city ILIKE '%Salé%' OR raw_city ILIKE '%Kénitra%' THEN 'Rabat-Salé-Kénitra'
      WHEN raw_city ILIKE '%Casablanca%' OR raw_city ILIKE '%Mohammedia%' OR raw_city ILIKE '%El Jadida%' OR raw_city ILIKE '%Settat%' OR raw_city ILIKE '%Berrechid%' THEN 'Casablanca-Settat'
      WHEN raw_city ILIKE '%Marrakech%' OR raw_city ILIKE '%Safi%' OR raw_city ILIKE '%Essaouira%' THEN 'Marrakech-Safi'
      WHEN raw_city ILIKE '%Fès%' OR raw_city ILIKE '%Meknès%' OR raw_city ILIKE '%Taza%' THEN 'Fès-Meknès'
      WHEN raw_city ILIKE '%Tanger%' OR raw_city ILIKE '%Tétouan%' OR raw_city ILIKE '%Larache%' OR raw_city ILIKE '%Ksar El Kebir%' THEN 'Tanger-Tétouan-Al Hoceïma'
      WHEN raw_city ILIKE '%Agadir%' OR raw_city ILIKE '%Taroudant%' THEN 'Souss-Massa'
      WHEN raw_city ILIKE '%Oujda%' OR raw_city ILIKE '%Nador%' THEN 'Oriental'
      WHEN raw_city ILIKE '%Béni Mellal%' OR raw_city ILIKE '%Khouribga%' THEN 'Béni Mellal-Khénifra'
      WHEN raw_city ILIKE '%Errachidia%' THEN 'Drâa-Tafilalet'
      WHEN raw_city ILIKE '%Laâyoune%' THEN 'Laâyoune-Sakia El Hamra'
      WHEN raw_city ILIKE '%Dakhla%' THEN 'Dakhla-Oued Ed-Dahab'
      ELSE 'Inconnue'
    END AS region,
    bank_location
  FROM extracted_locations
),

enriched_locations AS (
  SELECT
    city,
    region,
    'Maroc' AS country,
    bank_location 
  FROM cleaned_locations
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['city', 'region']) }} AS location_id,
  city,
  region,
  country,
  bank_location
FROM enriched_locations
GROUP BY city, region, country, bank_location
