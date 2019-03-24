"""vanhack.py: Query Faire API, compute inventory, query orders, evaluate if order can be fulfilled or not, answer general queries"""
__author__      = "Nicolas Veiga"

import requests
import json
import pandas as pd

API_KEY = 'HQLA9307HSLQYTC24PO2G0LITTIOHS2MJC8120PVZ83HJK4KACRZJL91QB7K01NWS2TUCFXGCHQ8HVED8WNZG0KS6XRNBFRNGY71'

def queryProducts(APIKey):
    products = []
    pageIndex = 1
    parameters = {"page": pageIndex}
    myUrl = "https://www.faire-stage.com/api/v1/products"
    head = {"X-FAIRE-ACCESS-TOKEN": APIKey,
            "Accept": "application/json"}
    try:
        response = requests.get(myUrl, params=parameters, headers=head)
    except:
        print("Something wrong with your internet connection, try again")
        return []

    if (response.status_code == 200):
        currentPage = response.json()["products"]
        products = currentPage

        while(len(currentPage) == response.json()["limit"]):
            pageIndex += 1
            parameters = {"page": pageIndex}
            try:
                response = requests.get(myUrl, params=parameters, headers=head)
            except:
                print("Something wrong with your internet connection, try again")
                return []

            if (response.status_code == 200):
                currentPage = response.json()["products"]
                products += currentPage
            else:
                print("Servers are down, try again")
                return []
    else:
        print("Servers are down, try again")
        return []

    return products

def queryOrders(API_KEY):
    orders = []
    pageIndex = 1
    parameters = {"page": pageIndex}
    myUrl = "https://www.faire-stage.com/api/v1/orders"
    head = {"X-FAIRE-ACCESS-TOKEN": API_KEY,
            "Accept": "application/json"}
    try:
        response = requests.get(myUrl, params=parameters, headers=head)
    except:
        print("Something wrong with your internet connection, try again")
        return []

    if (response.status_code == 200):
        currentPage = response.json()["orders"]
        orders = currentPage
        
        while(len(currentPage) == response.json()["limit"]):
            pageIndex += 1
            parameters = {"page": pageIndex}
            try:
                response = requests.get(myUrl, params=parameters, headers=head)
            except:
                print("Something wrong with your internet connection, try again")
                return []
            if (response.status_code == 200):
                currentPage = response.json()["orders"]
                orders += currentPage
            else:
                print("Something wrong with servers")
                return [] 
    else:
        print("Something wrong with servers")
        return []

    return orders

def updateState(ordersDataFrame, ableToFullfill, index, orderIds):
    if(not ableToFullfill):
        #TODO: should we be sending Backordering Items, sending a post command
        #https://www.faire.com/api/v1/orders/<ID>/items/availability
        index = ordersDataFrame[ordersDataFrame.id == orderIds.values[index]].index[0]
        ordersDataFrame.state.iloc[index] = "BACKORDERED"

def updateInventory(order, productsDataFrame):
    for item in order:
        #TODO: send a PATCH https://www.faire.com/api/v1/products/options/<ID>
        optionsDataFrame = pd.DataFrame.from_dict(productsDataFrame.options[productsDataFrame.id == item["product_id"]].values[0])
        index = optionsDataFrame[optionsDataFrame.id == item["product_option_id"]].index[0]
        optionsDataFrame.available_quantity.iloc[index] -= item["quantity"]
        
        index = productsDataFrame.options[productsDataFrame.id == item["product_id"]].index[0]
        productsDataFrame.options.iloc[index] = optionsDataFrame.to_dict("records")
        
def processOrders(ordersInProcessingState, inventory, ordersDataFrame, productsDataFrame):
    orderIds = ordersInProcessingState["id"]
    for index, order in enumerate(ordersInProcessingState["items"]):
        ableToFullfill = False
        for item in order:
            if(not inventory[(inventory["product_id"] == item["product_id"]) & 
                             (inventory["id"] == item["product_option_id"])].empty):
                if (item["quantity"] <= 
                    inventory[(inventory["product_id"] == item["product_id"]) & 
                              (inventory["id"] == item["product_option_id"])].available_quantity.values[0]):
                    ableToFullfill = True
                else:
                    ableToFullfill = False
                    break
            else:
                ableToFullfill = False
                break
        if(ableToFullfill):
            updateInventory(order, productsDataFrame)
        updateState(ordersDataFrame, ableToFullfill, index, orderIds)
        
def getBestSellingProductOption(ordersDataFrame):
    #Flat items list from ordersDataFrame
    itemsDataFrame = pd.DataFrame.from_dict([item for sublist in ordersDataFrame["items"].tolist() for item in sublist])
    #Group by product_id and product_option_id summing quantity and return a dataframe sorted in non ascending order
    bestSelling = itemsDataFrame.groupby(['product_id' ,'product_option_id']).sum().sort_values(by=['quantity'], ascending = False)
    return bestSelling.index[0][1]

def getLargestOrder(ordersDataFrame):
    #Flat items list from ordersDataFrame
    itemsDataFrame = pd.DataFrame.from_dict([item for sublist in ordersDataFrame["items"].tolist() for item in sublist])
    groupbyOrderId = itemsDataFrame.groupby(['order_id']).sum()
    orderValue = groupbyOrderId["price_cents"] * groupbyOrderId["quantity"]
    groupbyOrderId["total_price"] = orderValue
    largestOrderDataFrame = groupbyOrderId.sort_values(by=['total_price'], ascending = False).head()
    return largestOrderDataFrame.index[0]

def getStateWithMostOrders(ordersDataFrame):
    addressDataFrame = pd.DataFrame.from_dict(ordersDataFrame["address"].tolist())
    return addressDataFrame.state.value_counts().index[0]

def execute(API_KEY):
    productsDataFrame = queryProducts(API_KEY)
    if(len(productsDataFrame) > 0):
        productsDataFrame = pd.DataFrame.from_dict(productsDataFrame)
        
        # Grouping the options so we can have everythin in one table
        optionsDataFrame = pd.DataFrame.from_dict([item for sublist in productsDataFrame["options"].tolist() for item in sublist])
        
        inventory = optionsDataFrame[(optionsDataFrame.available_quantity.notnull()) & (optionsDataFrame.available_quantity > 0)]

        orders = queryOrders(API_KEY)
        if(len(orders) > 0):
            ordersDataFrame = pd.DataFrame.from_dict(orders)
            
            ordersSorteredProcessingState = ordersDataFrame[ordersDataFrame.state == "PROCESSING"]
            ordersSorteredProcessingState = ordersSorteredProcessingState.sort_values(by=['ship_after'])
            
            processOrders(ordersSorteredProcessingState, inventory, ordersDataFrame, productsDataFrame)
            
            #Queries
            print(getBestSellingProductOption(ordersDataFrame))
            print(getLargestOrder(ordersDataFrame))
            print(getStateWithMostOrders(ordersDataFrame))
        else:
            print("No Orders to process")
    else:
        print("No Products to process")

execute(API_KEY)

