# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 17:48:15 2026

@author: dipeng.chen
"""

import os
from stable_baselines3 import PPO
from env import BessTradingEnv
from feature_builder import build_training_dataset
import boto3


MODEL_PATH = "/tmp/bess_rl_policy.zip"


def train():

    df = build_training_dataset()

    env = BessTradingEnv(df)

    model = PPO(
        "MlpPolicy",
        env,
        verbose=1,
        learning_rate=0.0003,
        batch_size=256,
    )

    model.learn(total_timesteps=200000)

    model.save(MODEL_PATH)

    upload_model()


def upload_model():

    bucket = os.getenv("MODEL_BUCKET")

    s3 = boto3.client("s3")

    s3.upload_file(
        MODEL_PATH,
        bucket,
        "rl/bess_rl_policy.zip",
    )


if __name__ == "__main__":

    train()