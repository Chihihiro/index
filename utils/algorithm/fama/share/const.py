

FACTORS_NAME_ID = {
    # Fama Series
    "SMB(Fama)": "000001", "HML(Fama)": "000002", "MKT(Fama)": "000003",

    # Carhart Series
    "SMB(Carhart)": "000011", "HML(Carhart)": "000012", "MOM(Carhart)": "000013", "MKT(Carhart)": "000014",

    # Fama-French Series
    "SMB(FamaFrench)": "000021", "HML(FamaFrench)": "000022", "RMW(FamaFrench)": "000023", "CMA(FamaFrench)": "000024",
    "MKT(FamaFrench)": "000025"
}

FACTORS_ID_NAME = {v: k for k, v in FACTORS_NAME_ID.items()}
FACTORS_ID_NAME_WITHOUT_SUFFIX = {v: k.split("(")[0] for k, v in FACTORS_NAME_ID.items()}
