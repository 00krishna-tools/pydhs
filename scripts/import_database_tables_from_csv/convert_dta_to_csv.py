import pandas as pd

def main(filename):
    d = pd.read_stata(filename)
    d.to_csv(filename + '.csv')
    print('conversion complete')

if __name__ == "__main__":
    # execute only if run as a script
    main('RWwi42fl.dta')
