# -*- coding: utf-8 -*-
"""
Created on Mon Aug 14 17:32:11 2017

@author: Dipeng.Chen
"""

from pulp import *
import collections
import operator
import pandas as pd
import csv

import numpy as np



def StorageOpt(pricefile, granu, InvSize, BatSize, ChargeEff, DischEff):
 
    InvSize = InvSize*24/granu

    dur = BatSize/InvSize
    
    hrate = BatSize/dur

    LPoptimal = []
    Dispatch = []

    # print(len(pricefile))    
    for row in range(len(pricefile)):
    
        print(row)
    
        pricelist = pricefile.iloc[row].values
        
        if not np.isfinite(pricelist).all():
            bad_mask = ~np.isfinite(pricelist)
            print(f"[WARN] StorageOpt: row {row} has NaN/inf in price list; "
                  f"replacing {bad_mask.sum()} entries with 0.0")
            pricelist = np.nan_to_num(pricelist, nan=0.0, posinf=0.0, neginf=0.0)
        # pricelist = pricefile
        # print(pricelist)
        
        Hspread = {}
        for i in range(1, granu):
            for j in range(i+1, granu+1):
                Hspread["S{},{}".format(i,j)] = pricelist[j-1]*DischEff - pricelist[i-1]*ChargeEff
        # print(Hspread.keys())
                
        # print(Hspread)
        import re
        
        def atoi(text):
            return int(text) if text.isdigit() else text
        
        def natural_keys(text):
            '''
            alist.sort(key=natural_keys) sorts in human order
            http://nedbatchelder.com/blog/200712/human_sorting.html
            (See Toothy's implementation in the comments)
            '''
            return [ atoi(c) for c in re.split(r'(\d+)', text) ]      
                
        alist = list(Hspread.keys())
        alist.sort(key=natural_keys)
        # print(alist)
        
        Hspreadindex = []
        for index in range(len(alist)):
            Hspreadindex.append(Hspread[alist[index]])
        
        #print(Hspreadindex)
        
        #print(sorted(Hspread.items(), key = lambda x:(x[0][1], x[2])))
        Hspreadsorted = collections.OrderedDict(sorted(Hspread.items()))
        
        
        #print(Hspreadsorted)
        #print(len(Hspread))
        
        prob = LpProblem('HourlyStorage', LpMaximize)
        vij = [LpVariable("v{},{}".format(i,j), 0) for i in range(1,granu) for j in range(i+1, granu+1)]
    
        
        # print(vij)
        
        
        # add objective
        total_pnl = sum(v*s for (v,s) in zip(vij, Hspreadindex))
        # print(total_pnl)
        
        prob += total_pnl
        
        # add constraint
        # injection
        m=0

        for i in range(granu-1):

            total_hcharge = sum([vij[m+j] for j in range(granu-1-i)])
            # j = granu-1-i
            # print('j1',j)
            # m=m+j-i-1
            m = m + granu-1-i
            prob += total_hcharge <= hrate
 
        # withdraw
        for i in range(granu-1):
            v=[]
            v.append(vij[i])
            n=0
            for j in range(granu-2,granu-2-i,-1):
                n=n+j
                v.append(vij[i+n])
            total_hdischarge = sum(v)
            prob += total_hdischarge <= hrate
        # Storage level 
        p=0

        cumulist = []
        for k in range(granu-1):
            
            # print('p=',p)
            cumulist.append([vij[p+j] for j in range(granu-1-k)])

            cumul_list = [item for sublist in cumulist for item in sublist]
            
            
            remindex = range(k)
            newlist=list(remindex)

            
            for x in range(granu-2,max(granu-2-k,1),-1):

                newlist=[k+x for k in newlist[1:]]

                remindex = list(remindex)+newlist
                
            cumul_list2=list(cumul_list)
            
            
            for index in sorted(remindex, reverse=True):
                del cumul_list2[index]
            storlevel = sum(cumul_list2)

            prob += storlevel <= BatSize    

            p=p+granu-1-k
        print(prob)
        
        
        status = prob.solve()
        LPoptimal.append(value(prob.objective))
        # 始终按 vij 的固定顺序输出，避免 __dummy / presolve 造成变量缺失
        Dispatch.append([v.varValue for v in vij])
        
        # vlist 只需要设置一次（固定顺序）
        if row == 0:
            vlist = [v.name for v in vij]


  

    return([LPoptimal, Dispatch, vlist])

if __name__=="__main__":
    pricefile = [39.49, 33.05, 30.00, 27.82, 27.91, 27.44, 31.47, 37.52, 40.33, 66.01, 82.60, 75.40, 79.50, 80.00, 70.00, 65.00, 74.90, 100.00, 115.00, 93.65, 74.98, 65.00, 47.97, 34.06]
    granu=24
    InvSize=100.0
    BatSize=100.0
    ChargeEff=1.04
    DischEff=0.96
    [LPoptimal, Dispatch, vlist] = StorageOpt(pricefile, granu, InvSize, BatSize, ChargeEff, DischEff)