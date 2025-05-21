from rdflib.plugins.serializers.turtle import TurtleSerializer

SUBJECT = 0
VERB = 1
OBJECT = 2

class TurtleSerializerCustom(TurtleSerializer):
    short_name = "turtle"
    indentString = "    "

    def startDocument(self) -> None:
        self._started = True
        ns_list = sorted(self.namespaces.items())

        if ns_list and self._spacious:
            self.write("\n")
