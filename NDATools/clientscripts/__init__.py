import NDATools

if not NDATools.initialization_complete:
    NDATools.check_version()
    NDATools.create_nda_folders()
    NDATools.initialization_complete = True