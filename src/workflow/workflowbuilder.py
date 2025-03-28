from src.workflow.workflow import Workflow
from src.step.step import BaseStep
import logging



class WorkflowBuilder ():
    def __init__ (self, steps=None):
        self.steps = steps if isinstance(steps, list) else []



    def build (self):
        return Workflow(self.steps)



    def add (self, step=None):
        try:
            if step is None:
                raise ValueError("Step is None.\n")
            if not isinstance(step, BaseStep):
                raise TypeError(f"Step {type(step).__name__} should be child of {BaseStep.__name__}\n")
            if not callable(getattr(step, "run", None)):
                raise AttributeError (f"Step has no 'run' method.\n")
            self.steps.append(step)
            
        except Exception as err:
            current_step = current_step.__class__.__name__ if current_step else "Unknown"
            logging.exception(f"Function: WorkflowBuilder.add(). Step Failed: {step.__class__.__name__}. Error: {err}\n")
        return self