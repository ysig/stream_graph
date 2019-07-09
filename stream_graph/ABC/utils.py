def ABC_to_string(abc, columns=[]):
    if bool(abc):
        import pandas as pd
        out, columns, a = str(), list(columns), False
        if hasattr(abc, 'instantaneous'):
            if abc.instantaneous:
                columns += ['ts']
            else:
                columns += ['ts', 'tf']
                a = True
        if hasattr(abc, 'discrete') and abc.discrete:
            out += "Discrete "
        elif a:
            columns += ['interval-type']
        if hasattr(abc, 'weighted') and abc.weighted:
            out += "Weighted "
            columns += ['w']

        out += str(abc.__class__) + "\n"
        out += pd.DataFrame(list(abc), columns=columns).to_string(index=False)
        return out
    else:
        return "Empty " + str(abc.__class__)
