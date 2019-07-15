import rasterio as rio
import os
import csv


def main():
    """
    This function get elevation of all states as a tif format 
    and then convert them to csv, 
    the csv was used to create a table of elevation in 
    database 
    """
    lower48elevation = open("/Documents/insight/states_elevation_csv/lower48elevation.csv", 'w')
    workingDirectory = "/Documents/insight/states_elevation_tif/"
    for fileName in os.listdir(workingDirectory):
        if fileName.endswith('.tif'):
            state = fileName[0:2]
            raster = rio.open(workingDirectory + fileName)
            array = raster.read(1)
            rows = array.shape[0]
            cols = array.shape[1]
            print(state)
            for r in range(0, rows):
                for c in range(0, cols):
                    if  0<array[r,c]<4500:
                        row = []
                        row.append(state)
                        row.append(str(array[r,c]))
                        row = ','.join(row)
                        lower48elevation.write(row + '\n')
            raster.close()
    lower48elevation.close()
    print("Done!")

main()
