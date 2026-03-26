# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 11:18:17 2017

@author: Dipeng.Chen
"""

import csv
import re
import numpy as np





def Load_Disp(your_list, var_list, granu):
    # dispv = {}
    
    
    
    def _safe_float(x):
        """Convert to float, treating '' or None as 0.0."""
        try:
            return float(x)
        except (TypeError, ValueError):
            return 0.0
    
    dispv = np.zeros((granu, granu), dtype=object)
    # for k in range(len(var_list)):
    #     dispv[var_list[k]] = your_list[k]
        
        
    aggregDisp = []
    datam = len(your_list)
    datan = len(var_list[0])
    
    for m in range(datam):
        dispv = np.zeros((granu, granu), dtype=float)  # 每天重置
    
        for n in range(datan):
            varname = str(var_list[0][n])
            nums = re.findall(r"\d+", varname)
            if len(nums) < 2:
                continue  # 跳过 __dummy 等
    
            i, j = int(nums[0]), int(nums[1])
            if not (1 <= i <= granu and 1 <= j <= granu):
                continue
    
            dispv[i-1, j-1] = _safe_float(your_list[m][n])
    
    
        tempagg = []
        for i in range(granu):
            
            act = 0
            for j in range(i+1, granu):
    #            print float(dispv[i,j])
                # act = act + float(dispv[i,j])
                act = act + _safe_float(dispv[i, j])

            if i>0:
                for k in range(0,i):
                    act = act - _safe_float(dispv[k,i])
            
            tempagg.append(act)
        aggregDisp.append(tempagg)
        
    return(aggregDisp)
        

