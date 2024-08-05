def texto_a_decimal(valor :str)->float:
    try:
        return round(float(valor),2)
    except:
        return None ##none nulo 