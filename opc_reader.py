from opcua import Client, ua  # type: ignore
import time


class OPCReader:
    def __init__(self, url: str):
        if not url:
            raise ValueError("OPC-UA server URL is null or empty")

        self._client = Client(url)

        # Variables for managing the stop state
        self._is_stopped = False
        self._last_stop_state = False
        self._stop_time = 0
        self._stop_time_start = None

    def connect(self):
        while True:
            try:
                if not self._client:
                    raise ValueError("OPC-UA client instance is null")
                self._client.connect()
                break
            except Exception as e:
                print(f"Connection failed: {e}. Retrying in 5 seconds...")

    def lire_variables_opc(self, delay=30):
        while True:
            try:
                variables = {
                    "debit": self.client.get_node("ns=5;i=13").get_value(),
                    "conso": self.client.get_node("ns=5;i=19").get_value(),
                    "vit_tirage": self.client.get_node("ns=5;i=6").get_value(),
                    "vit_extrusion": self.client.get_node("ns=5;i=5").get_value(),
                    "poid_metre": self.client.get_node("ns=5;i=17").get_value(),
                    "etat_marche": self.client.get_node("ns=5;i=2").get_value(),
                    "etat_demarrage": self.client.get_node("ns=5;i=3").get_value(),
                    "etat_arret": self.client.get_node("ns=5;i=4").get_value(),
                }
                if variables:
                    return variables
            except AttributeError as e:
                print(f"Erreur d'accès à une propriété du client OPC UA: {e}")
            except Exception as e:
                print(f"Erreur de lecture des variables OPC UA: {e}")
                self.connect()
            time.sleep(delay)

    def ecrire_variable_opc(self, variable_name, value):
        try:
            node = self.client.get_node(f"ns=4;i={variable_name}")
            datavalue = ua.DataValue(ua.Variant(value, ua.VariantType.Boolean))
            node.set_value(datavalue)
            print(
                f"Variable OPC '{variable_name}' mise à jour avec la valeur '{value}'"
            )
        except AttributeError as e:
            print(f"Erreur d'accès à une propriété du client OPC UA: {e}")
        except Exception as e:
            print(
                f"An unexpected error occurred while writing to the OPC variable: {e}"
            )

    def disconnect(self):
        self.client.disconnect()

    # def check_etat_arret(self):

    #     return self.client.get_node(
    #         "ns=5;i=4"
    #     ).get_value()  # Exemple de lecture depuis un noeud OPC

    # def update_etat_arret(self):
    #     """Mettre à jour l'état d'arrêt et calculer le temps d'arrêt"""
    #     current_etat_arret = self.check_etat_arret()

    #     # Si l'état d'arrêt change de False à True, démarrer le comptage du temps d'arrêt
    #     if current_etat_arret and not self.last_etat_arret:
    #         self.start_art_time = time.time()  # Début du temps d'arrêt
    #         print("Etat d'arrêt détecté, démarrage du comptage du temps d'arrêt.")

    #     # Si l'état d'arrêt passe de True à False, ajouter le temps d'arrêt
    #     if not current_etat_arret and self.last_etat_arret:
    #         if self.start_art_time is not None:
    #             self.tps_art += (
    #                 time.time() - self.start_art_time
    #             )  # Ajouter le temps d'arrêt
    #             print(
    #                 f"Etat d'arrêt terminé, temps d'arrêt total : {self.tps_art:.2f} secondes."
    #             )
    #             self.start_art_time = None  # Réinitialiser le temps d'arrêt

    #     # Mettre à jour l'état précédent pour la prochaine itération
    #     self.last_etat_arret = current_etat_arret

    # def get_tps_art(self):
    #     """Retourner le temps d'arrêt total"""
    #     if self.etat_arret and self.start_art_time is not None:
    #         self.tps_art += (
    #             time.time() - self.start_art_time
    #         )  # Ajouter le temps si l'état d'arrêt est toujours actif
    #         self.start_art_time = (
    #             time.time()
    #         )  # Redémarrer le compteur pour éviter l'accumulation
    #     return self.tps_art

    # def set_etat_arret(self, etat):
    #     """Mettre à jour l'état d'arrêt"""
    #     self.etat_arret = etat
    #     self.update_etat_arret()  # Mettre à jour l'état et calculer le temps d'arrêt
