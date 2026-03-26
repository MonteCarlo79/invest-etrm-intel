# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 17:49:36 2026

@author: dipeng.chen
"""

from agents.rl.train import train


def run():

    print("Starting RL training job")

    train()

    print("Training complete")


if __name__ == "__main__":

    run()