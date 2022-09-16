# Lage ny aktivitet med koordinater

1. Gå til bruker
2. Loop gjennom `trajectory/<yyyyMMddHHmmss>.plt`
   1. Sjekk om det finnes en aktivitet i brukerens `labels.txt` der start time korresponderer med navn på trajectory-fil
      1. Hvis det korresponderer, bruk `labels.txt`-oppføringen til å lage activity'en, før alle punkter legges inn i den opprettede aktiviteten
      2. Hvis det ikke korresponderer, bruk første og siste punkt i trajectory til å lage en activity for alle punkter så legges inn

