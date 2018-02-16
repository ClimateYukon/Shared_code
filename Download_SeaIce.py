def get_files(url):
    '''this function open the url and extract the 'tif' urls'''
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(requests.get(url).text,"html.parser")
    _lnk = soup.find_all('a')[5:]
    return [os.path.join(url , a['href']) for a in _lnk if '.tif' in a['href']]
        
def download_file(ls):
    '''This function download a list of url, here those are the tifs produced by get_files,
    it save it in the same structure than the original for more clarity, also since the multiprocessing
    returned an error I added a test to be sure that all files are in there.'''
    
    import wget
    id_path = os.path.join( *ls[0].split('/')[-5:-1] )
    out_path = os.path.join(base_out_path , id_path)
    
    if not os.path.exists( out_path ):
        os.makedirs( out_path )

    for i in ls : 
        if not os.path.exists( os.path.join(out_path , os.path.basename( i ))):
            wget.download(i , out= out_path)
        else : print( 'File already exist'  )
        
def check_n_start( URL ) :
    '''A little helper to make multiprocessing work, this check if the url passed exists or not. To do so we 
    request the url and check the code returned by the server, if code = 200, the url exists and we can go ahead
    with extraction of tif url and download of the files'''
    
    request = requests.get( URL )
    if request.status_code == 200:
        download_file( get_files( URL ) )
        
    else : print('URL doesn't exist')    

if __name__ == '__main__':    
    import requests, os, itertools
    from multiprocessing import Pool
    import zipfile
    
    '''hardcording is ugly, however since it is a one off kind of thing, that was easier than writing a web crawler
    to try to find the right structure, still though... ugly'''
    
    # ds = ['n10m' , 'n3125' , 'n6250' , 's3125' , 's6250']
    ds= ['n6250']
    yr = [ str(i) for i in range(2012,2019)]
    ext = ['Arctic' , 'Arctic180']
    month = ['jan' , 'feb' , 'mar', 'apr' , 'may' , 'jun' , 'jul' , 'aug' , 'sep' , 'oct' , 'nov' , 'dec']

    base_out_path = '/home/UA/jschroder/Piia'

    url = 'https://seaice.uni-bremen.de/data/amsr2/asi_daygrid_swath/'
    product_generator = itertools.product([url] , ds , yr , month , ext)
    list_url = [ os.path.join( *pg ) for pg in product_generator ]


    pool = Pool( 32 )

    _ = pool.map( check_n_start, list_url )
    pool.close()
    pool.join()
    
    zf = zipfile.ZipFile(os,path.join( base_out_path , "full_extract.zip"), "w")
    for dirname, subdirs, files in os.walk("base_out_path"):
        zf.write(dirname)
        for filename in files:
        zf.write(os.path.join(dirname, filename))
    zf.close()
    
    # for lu in list_url :
    #     request = requests.get( lu )
    #     if request.status_code == 200:
    #         download_file( get_files( lu ) )
    #     else : print('not a viable url')
