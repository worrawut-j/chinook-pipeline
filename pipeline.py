# Databricks notebook source
import pandas as pd
import math

# file path
inputPath = "/Workspace/Users/worrawuj@ais.co.th/track_large.csv"
outputPath = "/Workspace/Users/worrawuj@ais.co.th/output_small.csv"

# Extract
tracks = pd.read_csv(inputPath)

# Transform
tracks["UnitPrice"] = tracks["UnitPrice"].apply(lambda x: math.ceil(x))
                             
# Load
tracks.to_csv(outputPath, index=False)
