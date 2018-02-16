def get_files(url):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(requests.get(url).text,"html.parser")
    _lnk = soup.find_all('a')[5:]
    return [os.path.join(url , a['href']) for a in _lnk if '.tif' in a['href']]
        
def download_file(ls):
    import wget
    id_path = os.path.join( *ls[0].split('/')[-5:-1] )
    out_path = os.path.join(base_out_path , id_path)
    
    if not os.path.exists( out_path ):
        os.makedirs( out_path )

    for i in ls : 
        if not os.path.exists( os.path.join(out_path , os.path.basename( i ))):
            wget.download(i , out= out_path)
        else : print( 'good to go, files is here'  )
def check_n_start(lu) :
    request = requests.get( lu )
    if request.status_code == 200:
        download_file( get_files( lu ) )
    else : print('not a viable url')    

if __name__ == '__main__':    
    import requests, os, itertools
    from multiprocessing import Pool
    
    # ds = ['n10m' , 'n3125' , 'n6250' , 's3125' , 's6250']
    ds= ['n6250']
    yr = [ str(i) for i in range(2012,2019)]
    ext = ['Arctic' , 'Arctic180']
    month = ['jan' , 'feb' , 'mar', 'apr' , 'jun' , 'jul' , 'aug' , 'sep' , 'oct' , 'nov' , 'dec']

    base_out_path = '/home/UA/jschroder/Piia'

    url = 'https://seaice.uni-bremen.de/data/amsr2/asi_daygrid_swath/'
    product_generator = itertools.product([url] , ds , yr , month , ext)
    list_url = [ os.path.join( *pg ) for pg in product_generator ]


    pool = Pool( 32 )

    _ = pool.map( check_n_start, list_url )
    pool.close()
    pool.join()
    
    # for lu in list_url :
    #     request = requests.get( lu )
    #     if request.status_code == 200:
    #         download_file( get_files( lu ) )
    #     else : print('not a viable url')
