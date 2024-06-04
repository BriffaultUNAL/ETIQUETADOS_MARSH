#!/usr/bin/python

import sys
import os

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, 'src')
sys.path.append(proyect_dir)

from src.utils import *
from src.telegram_bot import enviar_mensaje

if __name__ == "__main__":

    asyncio.run(enviar_mensaje('Etiquetado_Marsh'))
    load(transform(sql_query_1, df_recording_log_6, 15, 'inbound'))
    load(transform(sql_query_2, df_recording_log_41, 15, 'inbound'))
    load(transform(sql_query_3, df_recording_log_6, 45, 'outbound'))
    load(transform(sql_query_4, df_recording_log_41, 45, 'outbound'))
    asyncio.run(enviar_mensaje("____________________________________"))
    