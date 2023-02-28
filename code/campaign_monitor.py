from skynet.db.model import (CampaignTrigger, CampaignTriggerState,
                             Observation, ObsState, DatabaseSession)
from datetime import datetime, timedelta
# import time

"""
    Monitor all active campaign triggers by synchronizing exposures
    every 60s (or by the time set by the 'SYNC_PERIOD'). The loop is
    delayed by 1s (or by the time set by 'LOOP_DELAY') to prevent 
    excessive and unnecessary queries to the database.
"""

SYNC_PERIOD = 60.0
LOOP_DELAY = 1.0


def monitor_campaign_triggers():
    prev_sync_time = datetime.utcnow() - timedelta(seconds=2.0)

    while True:
        db = DatabaseSession()
        since_prev_sync_time = (datetime.utcnow() - prev_sync_time).total_seconds()
        if since_prev_sync_time > LOOP_DELAY:
            triggers = db.query(CampaignTrigger) \
                         .filter(CampaignTrigger.state==CampaignTriggerState.ACTIVE) \
                         .join(Observation).filter(Observation.state==ObsState.ACTIVE) \
                         .all()

            for trigger in triggers:
                if trigger.lastSynced:  # prevent race condition
                    if (datetime.utcnow() - trigger.lastSynced).total_seconds() > SYNC_PERIOD:
                        trigger.synchronize_exposures()
            prev_sync_time = datetime.utcnow()
        db.close()


if __name__ == '__main__':
    monitor_campaign_triggers()
