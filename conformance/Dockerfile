FROM dukegcb/calrissian:0.5.0
LABEL maintainer="dan.leehr@duke.edu"

# Become root again to install stuff
USER root

# To run conformance tests we must install cwltest and clone the tests
RUN pip install cwltest

# now switch back to calrissian
USER calrissian

CMD ["bash"]
