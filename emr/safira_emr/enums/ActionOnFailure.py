from enum import Enum

class ActionOnFailure(str, Enum):
    TerminateCluster = "TERMINATE_CLUSTER"
    CancelAndWait = "CANCEL_AND_WAIT"
    Continue = "CONTINUE"