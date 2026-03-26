# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 17:48:44 2026

@author: dipeng.chen
"""

import os
import boto3
from stable_baselines3 import PPO


MODEL_PATH = "/tmp/bess_rl_policy.zip"


def load_model():

    bucket = os.getenv("MODEL_BUCKET")

    s3 = boto3.client("s3")

    s3.download_file(
        bucket,
        "rl/bess_rl_policy.zip",
        MODEL_PATH,
    )

    model = PPO.load(MODEL_PATH)

    return model