# OpenZEV
Eine Software, mit der das Abrechnen in einem "Zusammenschluss zum Eigenverbrauch" (ZEV) erleichtert werden soll. Die Software soll die Verbrauchdaten aller Teilnehmer aus den Energiezählern auslesen können. Anschliessend die Verbrauchsdaten berechnen und eine Abrechnung erstellen. Ausserdem sollen die ausgelesenen Werte gecached werden, um Ladezeiten bei weiteren Anfragen zu minimieren.

## Geschichte/Ziel
In unserem Mehrfamilienhaus wurde eine Solaranlage installiert und der Hausanschluss in einen ZEV-Anschluss umgewandelt. Daher müssen wir nun selbst abrechnen. Dabei habe ich schnell gemerkt, dass es keine offensichtliche, gute Gratislösung gibt. Also habe ich entschieden das Problem selbst in die Hand zu nehmen. Eingebaut sind nun mehrere "EMU Professional II 3/5 TCP/IP" Energiezähler. Diese zeichnen die bezogene und eingespeiste Energie in einem 15min Takt auf. Diese Logs sind anschliessend für 3 Jahre über das Netzwerk auslesbar. Vorerst werde ich alles in Python schreiben. Später können aber auch noch weitere Sprachen dazu kommen.

## Stand
Ein Proof of Concept besteht. Dieses ist jedoch noch nicht geprüft und sehr manuell. Zur Prüfung der Korrektheit des Resultats wurde bis anhin die Energieseite eine HomeAssistants verwendet.

## Sonstiges
Wer Fragen hat oder einfach über das Projekt diskutieren will, kann gerne in den Diskussionen des Projektes vorbeischauen.