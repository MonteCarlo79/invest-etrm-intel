# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 22:34:56 2026

@author: dipeng.chen
"""

from shared.agents.execution_agent import generate_and_send_daily_report

if __name__ == "__main__":
    result = generate_and_send_daily_report(send_slack=True, send_email=True)
    print("Daily report sent.")
    print(result["pdf_path"])