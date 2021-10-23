# -*- coding: utf-8 -*-
"""
Created on Sun May 23 10:11:41 2021
@author: Julian
"""


from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import datetime
import math


from scr_common import UNI_v3_funcs as UNI_funcs

url='https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-prod'

query='''query pl($pool_id:String) 
    {pools(where: { id: $pool_id } ) 
        {
        token0 {symbol decimals}
        token1 {symbol decimals}
        feeTier
        feesUSD
        volumeUSD
        token0Price
        token1Price
        volumeToken0
        volumeToken1
        tick
        liquidity
        sqrtPrice
        ticks(first:500,skip:6) {id,price0,price1,tickIdx,liquidityGross,liquidityNet,volumeUSD}
        }
        }'''



'''GraphQl'''
def query_univ3(url,query_a,params):

    sample_transport=RequestsHTTPTransport(
       url=url,
       verify=True,
       retries=5,)
    client = Client(transport=sample_transport)
    query = gql(query_a)
    response = client.execute(query,variable_values=params)
    
    
    return response


def get_liquidity(pool_id,price_range,position):


    
    # Falta usar los decimales que nos da subgraph para cada token

    '''Calculating liquidity and amounts of token by tick'''
    
    # Query subgraph
    params ={"pool_id":pool_id}   
    a=query_univ3(url,query,params)
    # create df with pool_data and tick_data
    pool_data=a['pools'][0]
    tick_data=pd.io.json.json_normalize(a['pools'][0]['ticks'])
    del pool_data['ticks']

    # Making up pool_data not related to ticks
    decimal0=int(pool_data['token0']['decimals'])
    decimal1=int(pool_data['token1']['decimals'])
    feeTier=int(pool_data['feeTier'])/1000000
    tick_space=int(pool_data['feeTier'])*2/100
    pool_data['liquidity']=int(pool_data['liquidity'])
    pool_data['sqrtPrice']=int(pool_data['sqrtPrice'])
    pool_data['tick']=int(pool_data['tick'])
    
    # Calculating actual price
    pool_data['Price']=1.0001**pool_data['tick']/10**(decimal1-decimal0)
    
    # Making up tick_data 
    tick_data['tickIdx']=tick_data['tickIdx'].astype(int)
    tick_data['price1']=1.0001**tick_data['tickIdx']/10**(decimal1-decimal0)
    tick_data['price0']=1/tick_data['price1']
    tick_data['liquidityNet']=pd.to_numeric(tick_data['liquidityNet'],errors='coerce',downcast='signed')
    tick_data['liquidityGross']=pd.to_numeric(tick_data['liquidityGross'],errors='coerce',downcast='signed')

    tick_data=tick_data.sort_values(by=['tickIdx'], ascending=True)

    if position!={}:
    #-------------------------------------------------------
    # Introducing a simulated position that modify liquidity

        liquidityPerTick=position['liquidity']/(position['tickUpper']-position['tickLower'])
        # Calculate liquidity in each tickArea (10,60,200 ticks depend on the pool)
        liquidityPerArea=liquidityPerTick*tick_space
        position.update( { 'liquidityperArea':liquidityPerArea})
        print(liquidityPerArea)
        # We add position liquidity to actual liquidity
        tick_data['liquidityNet']=tick_data['liquidityNet']+position['liquidityperArea']


    # Calculating closing ticks to actual price and liquidity on active tick
    tick_space= int(pool_data['feeTier']) *2 /100 
    closest_ticks=(math.floor(pool_data['tick']/tick_space)*tick_space, math.ceil(pool_data['tick']/tick_space)*tick_space)
    pool_data['tickUpper']=closest_ticks[1]
    pool_data['tickLower']=closest_ticks[0]
    active_amounts=UNI_funcs.get_amounts(pool_data['tick'],closest_ticks[0],closest_ticks[1],pool_data['liquidity'],decimal0,decimal1)
    T1_onTick=active_amounts[1]+active_amounts[0]*float(pool_data['token1Price'])
    T0_onTick=active_amounts[1]*float(pool_data['token0Price'])+active_amounts[0]

    # Creating pa,pb range for a tick taking in consideration the direction of the pool
    # As uniswap UI paint as active zone till Upper tick of the active we split df like that
    tick_data['tickIdx_B']= np.where( tick_data['tickIdx']<=closest_ticks[1], tick_data['tickIdx'].shift(1), tick_data['tickIdx'].shift(-1)  )

    
    # Creating 2 tick_datas, one for token0 side, another for token1 side
    # As uniswap UI paint as active zone till Upper tick of the active we split df like that
    tick_data_token0=tick_data.loc[tick_data['tickIdx']>closest_ticks[1]]
    tick_data_token1=tick_data.loc[tick_data['tickIdx']<=closest_ticks[1]].sort_values(by=['tickIdx'], ascending=False)
    
    # Liquidity in each tick is equal to Activeliquidity +/- the rolling liquidityNet
    tick_data_token0['rolling_Net'] = tick_data_token0['liquidityNet'].rolling(100000, min_periods=1).sum()
    tick_data_token0=tick_data_token0.dropna()
    tick_data_token0['liquidity'] = pool_data['liquidity'] + tick_data_token0['rolling_Net']
    #Calculate for Liquitidy in each tick the amount of tokens
    tick_data_token0['amounts_Rolling'] = tick_data_token0.apply(lambda x: UNI_funcs.get_amounts(pool_data['tick'],int(x['tickIdx']),int(x['tickIdx_B']),int(x['liquidity']),decimal0,decimal1) ,axis=1)
    # Extract amount token 0 and amount token 1
    tick_data_token0['amount0']= [x[0] for x in tick_data_token0['amounts_Rolling']]
    tick_data_token0['amount1']= [x[1] for x in tick_data_token0['amounts_Rolling']]
    # Accumulated liquidity on amounts
    tick_data_token0['amount0_ac']=tick_data_token0['amount0'].cumsum()
    tick_data_token0['amount1_ac']=tick_data_token0['amount1'].cumsum()
    del tick_data_token0['amounts_Rolling']
    
    # Liquidity side 2 (As include the active zone some additional fix has to be made)
    tick_data_token1['rolling_Net'] = tick_data_token1['liquidityNet'].rolling(100000, min_periods=1).sum()
    tick_data_token1=tick_data_token1.dropna()
    tick_data_token1['liquidity'] = pool_data['liquidity']-tick_data_token1['rolling_Net']
    # Fix data on active zone
    tick_data_token1['liquidity'] = np.where(tick_data_token1['tickIdx']==closest_ticks[1] ,pool_data['liquidity'],tick_data_token1['liquidity'])
    tick_data_token1=tick_data_token1.sort_values(by=['tickIdx'])
    #Calculate for Liquitidy in each tick the amount of tokens
    tick_data_token1['amounts_Rolling'] = tick_data_token1.apply(lambda x: UNI_funcs.get_amounts(pool_data['tick'],int(x['tickIdx']),int(x['tickIdx_B']),int(x['liquidity']),decimal0,decimal1) ,axis=1)
    # Extract amount token 0 and amount token 1
    tick_data_token1['amount0']= [x[0] for x in tick_data_token1['amounts_Rolling']]
    tick_data_token1['amount1']= [x[1] for x in tick_data_token1['amounts_Rolling']]
    # Get amounts on the active zone and put it on the df
    tick_data_token1['amount0'] = np.where( tick_data_token1['tickIdx']==closest_ticks[1], active_amounts[0],tick_data_token1['amount0'])    
    tick_data_token1['amount1'] = np.where( tick_data_token1['tickIdx']==closest_ticks[1] ,active_amounts[1],tick_data_token1['amount1'])    
    # Accumulated liquidity on amounts (should be done on a inverse way)
    tick_data_token1['amount0_ac'] =tick_data_token1['amount0']
    tick_data_token1['amount1_ac'] = tick_data_token1.loc[::-1, 'amount1'].cumsum()[::-1]
    del tick_data_token1['amounts_Rolling']

    # Concatenate the two tick_datas and preparing data to be saved
    final_tick_data=pd.concat([tick_data_token1,tick_data_token0])
    final_tick_data['timestamp']=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  

    # Filter to a reasonable distance from price 
    lower_filter = closest_ticks[0]-price_range*(1/0.0001)
    upper_filter = closest_ticks[1]+price_range*(1/0.0001)
    final_tick_data=final_tick_data.loc[(final_tick_data['tickIdx']<= upper_filter ) & (final_tick_data['tickIdx']>=lower_filter )]


    final_tick_data['geometric_mean'] = (final_tick_data['tickIdx']*final_tick_data['tickIdx_B'])**(1.0/2)
    final_tick_data['price1_mean']=1.0001**final_tick_data['geometric_mean']/10**(decimal1-decimal0)
    final_tick_data['price0_mean']=1/final_tick_data['price1_mean']    
    

    return final_tick_data,pool_data




def get_tradeImpact(pool_id,tokenIn,qtyIn,position):
    

    # Fetching actual state of the pool liquidity 
    data = get_liquidity(pool_id,0.6,position) # 0.6 only filter tick value to a meaninful range can be 1 for full range
    tickData = data[0]  
    poolData = data[1] 
    
    feeTier=int(poolData['feeTier'])/1000000
    tick_space=int(poolData['feeTier'])*2/100
    decimal0=int(poolData['token0']['decimals'])
    decimal1=int(poolData['token1']['decimals'])

    qtyIn = [x*feeTier for x in qtyIn]

    final_prices={}
    # Looping through different swap size
    for qty in qtyIn:
                     
        if tokenIn == 1:
            # Filtering liquidity side to use
            # When token 1 is being swapped liquidity move from tick lower to tick upper
            tickData=tickData.loc[tickData['tickIdx'] >=poolData['tickUpper']]

            # Calculate qty of token 0 available in every zone
            # Fixing geometric mean in active zone (simplification of geometric mean as for a tick is more or less 1/2 of the difference)
            tickData['geometric_mean'] = np.where ( (tickData['amount0']>0) & (tickData['amount1']>0),
                                                    (tickData['tickIdx'].iloc[0]+poolData['tick'])/2,
                                                    (tickData['tickIdx']+tickData['tickIdx_B'])/2)    
            tickData['price1_mean']=1.0001**tickData['geometric_mean']/10**(decimal1-decimal0)
            tickData['price0_mean']=1/tickData['price1_mean']    
            
            # token1 will be swapped with price = geometric mean inside a tick zone
            tickData['amount1_available'] = tickData['amount0'] / tickData['price0_mean']
            tickData['amount1_available_ac'] = tickData['amount1_available'].cumsum()
            
            # Checking if swap cross active zone
            if qty > tickData['amount1_available_ac'].iloc[0]:
                # Getting active zone after swap
                tickData['final_zone'] =  qty < tickData['amount1_available_ac']
                tickData['final_zone'] = np.where((tickData['final_zone'] == True) & (tickData['final_zone'].shift(1) == False),1,0)
                zone_tickUpper = tickData.loc[tickData['final_zone']==1]['tickIdx_B']
                zone_tickLower = tickData.loc[tickData['final_zone']==1]['tickIdx']
                # qty that remains on the active zone
                already_swapped_idx = tickData[tickData['final_zone'] == 1].index.values.astype(int)[0]-1 
                qty_onActive =  qty - int(tickData.loc[tickData.index==already_swapped_idx]['amount1_available_ac'] ) # qty - already swapped qty 
                # Final tick after swapping
                final_tick = int(zone_tickLower + math.ceil( (qty_onActive/int(tickData.loc[(tickData['final_zone'] == 1)]['amount1_available']) ) *tick_space))
                initial_tick = poolData['tick']

            
            else:
                # In this case swap doesnt cross tick zones
                initial_tick = poolData['tick']
                final_tick = int(initial_tick + math.ceil( (qty/int(tickData['amount1_available'].iloc[0] ) * tick_space )))



        elif tokenIn == 0:
            
            # Filtering liquidity side to use
            # When token 0 is being swapped liquidity move from tick upper to tick lower
            tickData=tickData.loc[tickData['tickIdx'] <=poolData['tickUpper']].sort_values(by=['tickIdx'], ascending=False)
            tickData=tickData.reset_index(drop=True)
            # Calculate qty of token 1 available in every zone
            # Fixing geometric mean in active zone (simplification of geometric mean as for a tick is more or less 1/2 of the difference)
            tickData['geometric_mean'] = np.where ( (tickData['amount0']>0) & (tickData['amount1']>0),
                                                    (tickData['tickIdx'].iloc[0]+poolData['tick'])/2,
                                                    (tickData['tickIdx']+tickData['tickIdx_B'])/2)    

            tickData['price1_mean']=1.0001**tickData['geometric_mean']/10**(decimal1-decimal0)
            tickData['price0_mean']=1/tickData['price1_mean']   
            
            
            # token1 will be swapped with price = geometric mean inside a tick zone
            tickData['amount0_available'] = tickData['amount1'] / tickData['price1_mean']
            tickData['amount0_available_ac'] = tickData['amount0_available'].cumsum()

            # Checking if swap cross active zone
            if qty > tickData['amount0_available_ac'].iloc[0]:
                # Getting active zone after swap
                tickData['final_zone'] =  qty < tickData['amount0_available_ac']
                tickData['final_zone'] = np.where((tickData['final_zone'] == True) & (tickData['final_zone'].shift(1) == False),1,0)
                zone_tickUpper = tickData.loc[tickData['final_zone']==1]['tickIdx']
                zone_tickLower = tickData.loc[tickData['final_zone']==1]['tickIdx_B']
                # qty that remains on the active zone
                already_swapped_idx = tickData[tickData['final_zone'] == 1].index.values.astype(int)[0]-1 
                qty_onActive =  qty - int(tickData.loc[tickData.index==already_swapped_idx]['amount0_available_ac'] ) # qty - already swapped qty 
                # Final tick after swapping
                final_tick = int(zone_tickLower + math.ceil( (qty_onActive/int(tickData.loc[(tickData['final_zone'] == 1)]['amount0_available']) ) *tick_space))
                initial_tick = poolData['tick']
      
            
            else:
                # In this case swap doesnt cross tick zones
                initial_tick = poolData['tick']
                final_tick = int(initial_tick - math.ceil( (qty/int(tickData['amount0_available'].iloc[0] ) * tick_space )))
                initial_tick = poolData['tick']
                   

        # Dict with tick and both prices
        final_prices[qty] = {'ticks':(int(initial_tick),int(final_tick))}

        initial_price0= 1.0001**initial_tick/10**(int(poolData['token1']['decimals'])-int(poolData['token0']['decimals']))
        final_price0= 1.0001**final_tick/10**(int(poolData['token1']['decimals'])-int(poolData['token0']['decimals']))
        price0_swap= (initial_price0*final_price0)**(1/2)
        final_prices[qty].update({'price0':(initial_price0 ,final_price0)})
        final_prices[qty].update({'price0_swap': price0_swap }) # Geometric mean of range
        final_prices[qty].update({'price1':(1/initial_price0 ,1/final_price0)})
        final_prices[qty].update({'price1_swap':  1/price0_swap  })
        final_prices[qty].update({'price_impact(%)': round( (initial_price0 - final_price0) / initial_price0 * 100 ,2) })
        final_prices[qty].update({'price_impact_swap(%)': round( (initial_price0 - price0_swap) / initial_price0 * 100 ,2) })
        if tokenIn == 1:
            final_prices[qty].update({'qty_received': round( (qty/price0_swap) ,2) })
        elif tokenIn == 0:
            final_prices[qty].update({'qty_received': round( (qty/(1/price0_swap)) ,2) })




    return final_prices,poolData



def main(pool_id,tokenIn,qtyIn,position):            
    # Calculating impact without position
    results=get_tradeImpact(pool_id,tokenIn,qtyIn,position={})
    trade_impact = results[0]
    trade_impact_df = pd.DataFrame.from_dict(trade_impact, orient='index')
    trade_impact_df['type']="No_Position"

    #Getting pool data to format position data
    poolData=results[1]
    decimal0=int(poolData['token0']['decimals'])
    decimal1=int(poolData['token1']['decimals'])
    symbol0=poolData['token0']['symbol']
    symbol1=poolData['token1']['symbol']
    #Amounts of new position
    amounts=UNI_funcs.get_amounts(poolData['tick'],position['tickLower'],position['tickUpper'],position['liquidity'],decimal0 ,decimal1)

    # New price impact calcs
    trade_impact_new = get_tradeImpact(pool_id,tokenIn,qtyIn,position)[0]
    trade_impact_new_df = pd.DataFrame.from_dict(trade_impact_new, orient='index')
    trade_impact_new_df['type']="Position"

    final_df=pd.concat([trade_impact_df,trade_impact_new_df])

    for qty in final_df.index.unique():
        symbol=symbol0 if tokenIn==0 else symbol1
        print("--- Actual price impact ---")
        print("--- Swapping %s of token %s you'll have a price impact of %s percent" %(qty,symbol,trade_impact[qty]['price_impact_swap(%)']))
        print("--- Price impact with new position---")
        print("----Adding %s-%s and %s-%s" %(amounts[0],symbol0,amounts[1],symbol1))
        print("----Swapping %s %s after adding you'll have a price impact of %s percent" %(qty,symbol,trade_impact_new[qty]['price_impact_swap(%)']))
        print("----Adding the position improved price impact by %s percent" %(round(trade_impact_new[qty]['price_impact_swap(%)']-trade_impact[qty]['price_impact_swap(%)'],2)))

    return final_df


# Address of the uniswap v3 pool to analyze
pool_id = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8".lower()
# Token to swap (i.e if pair is USDC/WETH USDC=0)
tokenIn = 1
# Qty of tokens to swap (must be adapted to the token to swap)
zeros_toTest = (4,7)
qtyIn = [1*10**x for x in range(zeros_toTest[0],zeros_toTest[1]+1)]

# Position that will modify actual liquidity
# Note: once script is running it will print token amounts of the position
position = {'liquidity':10**18,
            'tickLower':191580,
            'tickUpper':193860  }

# Results will have a df with all price impact results
results=  main(pool_id,tokenIn,qtyIn,position)      



