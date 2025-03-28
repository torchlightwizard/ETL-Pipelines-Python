from src.step.step import BaseStep
import logging



class Workflow ():
    def __init__ (self, steps=[]):
        current_step = None
        try:
            if not isinstance(steps, list):
                raise TypeError(f"Require a list of steps.\n")
            
            for step in steps:
                current_step = step
                if step is None:
                    raise ValueError("Step is None.\n")
                if not isinstance(step, BaseStep):
                    raise TypeError(f"Step {type(step).__name__} should be child of {BaseStep.__name__}\n")
                if not callable(getattr(step, "run", None)):
                    raise AttributeError(f"Step has no 'run' method.\n")
            self.steps = steps

        except Exception as err:
            current_step = current_step.__class__.__name__ if current_step else "Unknown"
            logging.exception(f"Function: WorkflowBuilder.build(). Step Failed: {current_step}. Error: {err}\n")
            self.steps = []



    def run (self):
        data = None
        for step in self.steps:
            try:
                data = step.run(data)

            except Exception as err:
                logging.exception(f"Function: Workflow.run(). Step Failed: {step.__class__.__name__}. Error: {err}\n")
        return self