#! /usr/bin/env python

''' Author: Nathan Thomas, Paul Montesano
    Date: 02/003/2020
    Version: 1.0
    THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.'''

from maap.maap import MAAP
import argparse

def getparser():
    parser = argparse.ArgumentParser(description='Correct CHM pixel values according to correction based on guassian peak analysis of minimum (ground) peak in CHM image histogram')
    parser.add_argument('data_dir', type=str, default=None, help='Data directory to which you download')
    parser.add_argument('bbox', type=str, default=None, help="Lon,lat box ('lx,ly,ux,uy')"")
    parser.add_argument('gran_limit', type=int, default=None, help="Limit of granules to return")                    
    return parser

def searth_atl08():
    maap = MAAP()                    
    parser = getparser()
    args = parser.parse_args()
                        
    if args.data_dir is None or args.bbox is None:
        print("Needs a data dir and a bounding box. Exiting")
        os._exit(1)
    
    existingfiles = [file for file in os.listdir(args.data_dir)]
    
    s=maap.searchGranule(collection_concept_id="C1200116818-NASA_MAAP", bounding_box=args.bbox, limit=10)
    
    for f in range(len(s)):
        ID = s[f]['Granule']['DataGranule']['ProducerGranuleId']
       if ID in existingfiles:
        print('file exists...')
       else:                        
        s[f].getLocalPath(args.data_dir)
    
def main():
                        
    print("\nICESat2GRD is written by Nathan Thomas (@Nmt28).\nModified by Paul Montesano | paul.m.montesano@nasa.gov\nUse '-h' for help and required input parameters\n")

    searth_atl08()


if __name__ == "__main__":
    main()
