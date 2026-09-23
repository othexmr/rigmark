"""Small review exercise: inspect cancellation and duplicate submissions."""
class Queue:
    def __init__(self):
        self.pending = []
        self.running = {}

    def submit(self, request_id, value):
        self.pending.append((request_id, value))

    def start(self):
        request_id, value = self.pending.pop(0)
        self.running[request_id] = value
        return request_id, value

    def cancel(self, request_id):
        self.running.pop(request_id, None)

    def finish(self, request_id):
        return self.running.pop(request_id)
