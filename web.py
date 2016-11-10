from multiprocessing import Process

from .themer import run

@app.route("/")
def exampleMethod():
    # Make the scraping asynchronous for not timing out the http-request
    p = Process(target=run) #args=('bob',)
    p.start()
    return "Theming"
