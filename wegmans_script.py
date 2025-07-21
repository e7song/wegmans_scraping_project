import requests
import time
import csv
import argparse

def verifyStore(storeNum : int):
    """
    Grabs the most recent stores from https://www.wegmans.com/api/stores to verify if the queried store id exists.
    
    """
    URL = "https://www.wegmans.com/api/stores"

    headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.wegmans.com/stores",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    }

    resp = requests.get(URL, headers=headers)
    resp.raise_for_status() # sanity check 
    stores = resp.json()

    for store in stores:
        if store["storeNumber"] == storeNum:
            print(f"The queried store is {store['name']}.")
            return True
    
    print("ERR: This store number does not exist.")
    return False

def getCategories(storeId : int):
    """
    Get a list of all the subcategories that have data from a specific store id.

    """
    headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.wegmans.com/shop/categories",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
        )
    }

    url = f"https://www.wegmans.com/api/categories/{storeId}"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status() # sanity check 
    storeData = resp.json()
    categories = []
    for mainDepartment, subDepartments in storeData["subCategoriesWithData"].items():
        categories.extend([cat["key"] for cat in subDepartments])
    return categories

def constructSession():
    """
    Builds a session for querying multiple times.
    
    """
    API_KEY = "9a10b1401634e9a6e55161c3a60c200d"
    APP_ID = "QGPPR19V8V"
    URL = f"https://{APP_ID}-dsn.algolia.net/1/indexes/*/queries"

    headers = {
        "x-algolia-api-key": API_KEY,
        "x-algolia-application-id": APP_ID,
        "user-agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " 
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/138.0.0.0 Safari/537.36"
        ),
        "content-type": "application/json",
        "origin": "https://www.wegmans.com",
        "referer": "https://www.wegmans.com/",
    }
    session = requests.Session()
    session.headers.update(headers)

    return session

def getAllBrands(subId : str, storeId : int, facetName = 'consumerBrandName',pageNum = 0, session = None):
    """
    Some subcategories have >1000 products. Further subdivision can be handled by grabbing consumer brand names.

    """
    
    if session == None:
        session = constructSession()

    APP_ID = "QGPPR19V8V"
    URL = f"https://{APP_ID}-dsn.algolia.net/1/indexes/*/queries"

    params = (
        "query=&"
        f"page={pageNum}&" 
        f"hitsPerPage={0}&"
        f"filters=storeNumber:{storeId} AND category.key:{subId} AND "
        "excludeFromWeb:false AND isSoldAtStore:true AND fulfilmentType:instore&"
        f"facets=%5B%22{facetName}%22%5D&"
        "maxValuesPerFacet=1000"
    )

    payload = {
        "requests": [
            {
                "indexName": "products",
                "params": params
            }
        ]
    }

    resp = session.post(URL, json=payload)
    resp.raise_for_status()
    res = resp.json()["results"][0]
    return res['facets']['consumerBrandName']

def filteredQuery(subId : str, storeId : int, facet_filter, hits = 750, session = None):
    
    if session == None:
        session = constructSession()

    APP_ID = "QGPPR19V8V"
    URL = f"https://{APP_ID}-dsn.algolia.net/1/indexes/*/queries"

    params = (
        "query=&"
        f"page={0}&" # hitsPerPage should cover everything if the subcategories are the ones being explored
        f"hitsPerPage={hits}&"
        f"filters=storeNumber:{storeId} AND category.key:{subId} AND "
        "excludeFromWeb:false AND isSoldAtStore:true"
        " AND fulfilmentType:instore"
    )

    payload = {
        "requests": [
            {
                "indexName": "products",
                "params": params,
                "facetFilters": [facet_filter]
            }
        ]
    }      
    
    resp = session.post(URL, json=payload)
    resp.raise_for_status()
    res = resp.json()["results"][0]

    return res['hits']

def getSubCategoryData(subId : str, storeId : int, hits = 750, brandName = None, session = None):
    """
    Given a store id and a subcategory id, get the data in the form of a dictionary.

    """
    
    if session == None:
        session = constructSession()

    APP_ID = "QGPPR19V8V"
    URL = f"https://{APP_ID}-dsn.algolia.net/1/indexes/*/queries"

    params = (
        "query=&"
        f"page={0}&" # hitsPerPage should cover everything if the subcategories are the ones being explored
        f"hitsPerPage={hits}&"
        f"filters=storeNumber:{storeId} AND category.key:{subId} AND "
        "excludeFromWeb:false AND isSoldAtStore:true AND fulfilmentType:instore"
    )

    if brandName:
        payload = {
            "requests": [
                {
                    "indexName": "products",
                    "params": params,
                    "facetFilters": [[f"consumerBrandName:{brandName}"]]
                }
            ]
        }      
    else:
        payload = {
            "requests": [
                {
                    "indexName": "products",
                    "params": params
                }
            ]
        }


    resp = session.post(URL, json=payload)
    resp.raise_for_status()
    res = resp.json()["results"][0]

    allHits = []

    # edge case where a subcategory nbHits > Hits
    if res['nbHits'] > len(res['hits']):
        brand_dct = getAllBrands(subId, storeId)

        curr_filter = []
        curr_sum = 0
        for brand, total in brand_dct.items():
            if curr_sum + total >= hits:
                curr_sum = total

                # do the query, add to allHits
                allHits.extend(filteredQuery(subId, storeId, curr_filter, session = session))

                curr_filter = [f'consumerBrandName:{brand}']
            else:
                curr_filter.append(f'consumerBrandName:{brand}')
                curr_sum += total
        
        if curr_filter: # still some categories left
            allHits.extend(filteredQuery(subId, storeId, curr_filter, session = session))
        
    else:
        allHits = res['hits']
    


    # print("all:" + str(len(allHits)) +  " nbHits: " + str(res['nbHits']))
    return allHits


fieldnames = [
    "productName",
    "upc",
    "lastUpdated",
    "productKeywords",
    "isAvailable",
    "consumerBrandName",
    "popularTags",
    "filterTags",
    "webProductDescription",
    "price_inStore_unitPrice",
    "price_inStore_amount",
    "skuId",
    "taxCode",
    "packSize",
    "productId",
    "productDescription"
]

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description= "Scrape all products for a given Wegmans store."
    )

    parser.add_argument(
        "store_id",
        type= int,
        help= "Wegmans store number (e.g. 156 for Astor Place)"
    )

    parser.add_argument(
        "-o", "--output",
        type= str,
        default= None,
        help= "Path to output CSV (default: wegmans_store_<storeId>_<timestamp>.csv)"
    )


    args = parser.parse_args()
    store_id = args.store_id
    timestamp = int(time.time())

    out_path = None
    if args.output:
        out_path = args.output
    else:
        out_path = f"wegmans_store_{store_id}_{timestamp}.csv"

    STORE_ID = store_id

    if not verifyStore(STORE_ID):
        parser.error(f'Invalid Store ID: {STORE_ID}.')

    session = constructSession()
    allCategories = getCategories(STORE_ID)

    numProds = 0
    start = time.time()
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames = fieldnames, lineterminator="\n")
        writer.writeheader()

        for subcategory in allCategories:
            subData = getSubCategoryData(subcategory, STORE_ID, session = session)
            for product in subData:
                numProds += 1
                row = {}
                for attr in fieldnames:
                    if attr.startswith("price_inStore_"):
                        subkey = attr.split("_")[-1]  
                        row[attr] = product.get("price_inStore", {}).get(subkey, "")
                    else:
                        row[attr] = product.get(attr, "")
                writer.writerow(row)
            time.sleep(0.2)
    end = time.time()

    print(f"{numProds} products scraped in {round(end - start, 2)}s.")

