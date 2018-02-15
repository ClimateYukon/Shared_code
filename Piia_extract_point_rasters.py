import rasterstats, os , glob
from rasterstats import point_query
import geopandas as gpd
import pandas as pd
import numpy as np

shp_folder = "E:/Training/Arctic/Piia_data/ASMR2_data/AMSR2_data_folders_training_/test01_2016_/"
tif_folder = "E:/Training/Arctic/Piia_data/ASMR2_data/AMSR2_data_folders_training_/test01_2016_/"
output_path = "E:/Training/Arctic/Piia_data/ASMR2_data/AMSR2_data_folders_training_/test01_2016_/Outputs"

if not os.path.exists( output_path ):
    os.mkdir( output_path )
    
tif_ls = glob.glob( os.path.join( tif_folder , '*.tif' )) #look for tifs in the folder
shp_ls = glob.glob( os.path.join( shp_folder , '*.shp' )) #look for shp in the folder


shps = [gpd.read_file(f) for f in shp_ls ] # here we are opening each shapefile and storing them in a list, that is a list comprehension, very powerful and useful instead of loops
# it is equivalent to :

# shps = [] #create an empty list
# for shp in shp_ls :    #loop through the filename in the shapefile list
#		shps.append(gpd.read_file(shp)) #open the shapefile and store the object in the list

# That is where the magic happens, that is called a dictionnary comprehension except this one is fancy as the
# values are made of a list comprehension
# Basically for each filename, basename extract only the filename from the full path, then we split to get just the numbers
# with split which gives us our key. For the value we apply the function point_query to each shapefile and fn (filename)
# by doing that we avoid creating two empty list and creating two loops etc.. it is way cleaner and faster!


dic = {os.path.basename(fn).split('_')[-2] : [ point_query( shp, fn ) for shp in shps] for fn in tif_ls}

#pandas is smart enough to build dataframe on the fly, you just have to tell which way you want your index oriented
df = pd.DataFrame.from_dict( dic , orient='index' )

# here I just set the columns name with yet another list comprehension grabing the filename from the shp_ls to have decent column name
df.columns = [ os.path.basename( shp ) for shp in shp_ls ] 

#just saving it as csv, if output path doesnt exist it will crash the code that is why we check that on line 11-12
#saying if path doesnt exist well create it
df.to_csv( os.path.join( output_path ,'extract_points_result.csv' ))

#here we get into the tricky things, its pretty advanced
# json.load is a hack to handle the list stored as a string in the datadrame.. it is weird and hacky but it works
# we use numpy (np) to take the mean of the list and we apply it to the full dataframe with applymap
df2 = df.applymap(lambda x: np.mean(json.loads(x))
df2.to_csv(os.path.join( output_path ,'extract_points_result_mean.csv'))
