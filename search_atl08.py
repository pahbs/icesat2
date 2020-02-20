#! /usr/bin/env python

''' Author: Nathan Thomas, Paul Montesano
    Date: 02/003/2020
    Version: 1.0
    THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.'''

from maap.maap import MAAP

def searth_atl08():
    maap = MAAP()
    
    s=maap.searchGranule(collection_concept_id="C1200116818-NASA_MAAP", bounding_box="95,70,100,75", limit=10)
    
    for f in range(len(s)):
       s[f].getLocalPath('/projects/data/atl08')
    
def main():
    print("\nICESat2GRD is written by Nathan Thomas (@Nmt28).\nModified by Paul Montesano | paul.m.montesano@nasa.gov\nUse '-h' for help and required input parameters\n")

    searth_atl08()


if __name__ == "__main__":
    main()