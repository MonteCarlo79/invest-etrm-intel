# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 17:46:19 2026

@author: dipeng.chen
"""

import gym
import numpy as np


class BessTradingEnv(gym.Env):

    def __init__(self, df):

        self.df = df.reset_index(drop=True)

        self.index = 0

        self.soc = 0.5

    def reset(self):

        self.index = 0
        self.soc = 0.5

        return self._state()

    def step(self, action):

        row = self.df.iloc[self.index]

        price = row.price

        reward = 0

        if action == 1:  # charge
            reward = -price

        if action == 2:  # discharge
            reward = price

        self.index += 1

        done = self.index >= len(self.df) - 1

        return self._state(), reward, done, {}

    def _state(self):

        row = self.df.iloc[self.index]

        return np.array([
            row.price,
            row.wind_forecast,
            row.load_forecast,
            self.soc
        ])