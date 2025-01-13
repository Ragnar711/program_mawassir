from datetime import datetime


def get_poste():
    current_time = datetime.now()
    if current_time is None:
        raise AttributeError("datetime object is None")
    if not isinstance(current_time, datetime):
        raise ValueError("datetime object is invalid")
    if 6 <= current_time.hour < 14:
        return "MATIN"
    elif 14 <= current_time.hour < 22:
        return "SOIR"
    else:
        return "NUIT"


def calculer_kpi(variables_opc, variables_sql):
    try:
        if variables_sql["of"]["debit"] <= 0:
            TP = 0
        else:
            TP = min((variables_opc["debit"] / variables_opc["debit"]) * 100, 100)

        if (variables_opc["conso"] - variables_sql["qte_nc"]) <= 0:
            TQ = 0
        else:
            TQ = min(
                (
                    (variables_opc["conso"] - variables_sql["qte_nc"])
                    / (variables_opc["conso"])
                )
                * 100,
                100,
            )

        # # Calcul du KPI TD (Taux de Disponibilité)
        # tps_ouv = (
        #     datetime.now() - start_time
        # ).total_seconds()  # Calcul du temps d'ouverture en secondes

        # # On évite la division par zéro
        # if tps_ouv <= 0 or (tps_ouv - tps_prog) == 0:
        #     TD = 0
        # else:
        #     # Calcul de la disponibilité en fonction du temps d'arrêt et du temps programmé
        #     TD = (tps_ouv - opc_reader.get_tps_art()) / (tps_ouv - tps_prog)
        #     TD = max(0, min(TD, 1))  # Limiter entre 0 et 1

        return {"TP": TP, "TQ": TQ, "TD": 0}

    except (ZeroDivisionError, ValueError, TypeError) as e:
        print(f"Erreur lors du calcul des KPI : {e}")
        return None, None, None, None
