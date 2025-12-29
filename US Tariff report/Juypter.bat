@echo off
REM Activate the base environment
call E:\softwares\anaconda3\condabin\activate base

REM Start Jupyter Notebook in the specified directory
start "" jupyter notebook --notebook-dir="E:/_Projects/US_Shipment_Bklg_Forcast"