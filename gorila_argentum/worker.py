from .ingest import run_batch
from .coupling import build_matrix

def main():
    print({"ingest":run_batch()})
    pairs=[("USD_MEP","sell","USD_CCL","sell"),("USD_BLUE","sell","USD_MEP","sell"),("USD_MEP","sell","EMBI_ARG","embi_bps"),("USD_CCL","sell","EMBI_ARG","embi_bps"),("USD_MEP","sell","USD_BCRA","reference")]
    print({"coupling":build_matrix(pairs)})

if __name__=="__main__": main()
