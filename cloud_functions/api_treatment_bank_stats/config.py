config = {
    'dioStats': {
        'basic_stats': {
            'query': ("https://tb.plazi.org/GgServer/dioStats/stats?"
                        "outputFields="
                            "doc.articleUuid+doc.name+doc.doi+"
                            "doc.uploadUser+doc.uploadDate+doc.uploadYear+doc.uploadMonth+"
                            "doc.updateUser+doc.updateDate+doc.updateYear+doc.updateMonth+"
                            "bib.title+bib.year+bib.source+bib.volume+bib.issue+bib.numero+"
                            "bib.firstPage+bib.lastPage+"
                            "cont.pageCount+cont.treatCount+cont.treatCitCount+cont.matCitCount+"
                            "cont.figCount+cont.tabCount+cont.bibRefCitCount"
                        "&groupingFields="
                            "doc.articleUuid+doc.name+doc.doi+"
                            "doc.uploadUser+doc.uploadDate+doc.uploadYear+doc.uploadMonth+"
                            "doc.updateUser+doc.updateDate+doc.updateYear+doc.updateMonth+"
                            "bib.title+bib.year+bib.source+bib.volume+bib.issue+bib.numero+"
                            "bib.firstPage+bib.lastPage"
                        "&orderingFields=doc.uploadDate"
                        "{delta}"
                        "&format={format}"),
            'fields': ['DocArticleUuid',
                       'DocName',
                       'DocDoi',
                       'DocUploadUser',
                       'DocUploadDate',
                       'DocUploadYear',
                       'DocUploadMonth',
                       'DocUpdateUser',
                       'DocUpdateDate',
                       'DocUpdateYear',
                       'DocUpdateMonth',
                       'BibTitle',
                       'BibYear',
                       'BibSource',
                       'BibVolume',
                       'BibIssue',
                       'BibNumero',
                       'BibFirstPage',
                       'BibLastPage',
                       'ContPageCount',
                       'ContTreatCount',
                       'ContTreatCitCount',
                       'ContMatCitCount',
                       'ContFigCount',
                       'ContTabCount',
                       'ContBibRefCitCount'],
            'delta_fields': [{'name': 'DocUploadYear',
                            'code': 'doc.uploadYear'},
                            {'name': 'DocUploadMonth',
                            'code': 'doc.uploadMonth'},
                            {'name': 'DocUpdateYear',
                            'code': 'doc.updateYear'},
                            {'name': 'DocUpdateMonth',
                            'code': 'doc.updateMonth'},
                            {'name': 'DocUploadDate',
                            'code': 'doc.uploadDate'},
                            {'name': 'DocUpdateDate',
                            'code': 'doc.updateDate'}],
            # 'params': {
            #         "load_mode": "FULL",
            #         "zone": "RAW",
            #         "project_id": "poc-plazi-raw",
            #         "dataset": "apis",
            #         "table": "tb_diostats_publication_stats",
            #     }
            },
        'authors': {
            'query': ("https://tb.plazi.org/GgServer/dioStats/stats?"
                    "outputFields="
                        "doc.articleUuid+doc.updateDate+auth.name+auth.aff+auth.email+"
                        "auth.lsid+auth.orcid+auth.url"
                    "&groupingFields="
                        "doc.articleUuid+doc.updateDate+auth.name+auth.aff+auth.email+"
                        "auth.lsid+auth.orcid+auth.url"
                    "{delta}"
                    "&format={format}"),
            'fields': ["DocArticleUuid",
                        "DocUpdateDate",
                        "AuthName",
                        "AuthAff",
                        "AuthEmail",
                        "AuthLsid",
                        "AuthOrcid",
                        "AuthUrl"],
            'delta_fields': [{'name': 'DocUploadYear',
                            'code': 'doc.uploadYear'},
                            {'name': 'DocUploadMonth',
                            'code': 'doc.uploadMonth'},
                            {'name': 'DocUpdateYear',
                            'code': 'doc.updateYear'},
                            {'name': 'DocUpdateMonth',
                            'code': 'doc.updateMonth'},
                            {'name': 'DocUploadDate',
                            'code': 'doc.uploadDate'},
                            {'name': 'DocUpdateDate',
                            'code': 'doc.updateDate'}],
            # 'params': {
            #             "load_mode": "FULL",
            #             "zone": "RAW",
            #             "project_id": "poc-plazi-raw",
            #             "dataset": "apis",
            #             "table": "tb_diostats_publication_authors",
            #     }
            }
        },
    'srsStats': {
        'basic_stats': {
            'query': ("https://tb.plazi.org/GgServer/srsStats/stats?"
                        "outputFields="
                            "doc.uuid+doc.articleUuid+doc.uploadUser+doc.uploadDate+"
                            "doc.updateUser+doc.updateDate+tax.kingdomEpithet+tax.phylumEpithet+"
                            "tax.classEpithet+tax.orderEpithet+tax.familyEpithet+tax.genusEpithet+"
                            "tax.speciesEpithet+tax.authName+tax.authYear+tax.status+"
                            "tax.isKey+cit.treatCitCount+cit.bibRefCitCount+cit.figCitCount+"
                            "cit.tabCitCount+mat.matCitCount"
                        "&groupingFields="
                            "doc.uuid+doc.articleUuid+doc.uploadUser+doc.uploadDate+"
                            "doc.updateUser+doc.updateDate+tax.kingdomEpithet+tax.phylumEpithet+"
                            "tax.classEpithet+tax.orderEpithet+tax.familyEpithet+tax.genusEpithet+"
                            "tax.speciesEpithet+tax.authName+tax.authYear+tax.status+tax.isKey"
                        "{delta}"
                        "&format={format}"),
            'fields': ["DocUuid",
                        "DocArticleUuid",
                        "DocUploadUser",
                        "DocUploadDate",
                        "DocUpdateUser",
                        "DocUpdateDate",
                        "TaxKingdomEpithet",
                        "TaxPhylumEpithet",
                        "TaxClassEpithet",
                        "TaxOrderEpithet",
                        "TaxFamilyEpithet",
                        "TaxGenusEpithet",
                        "TaxSpeciesEpithet",
                        "TaxAuthName",
                        "TaxAuthYear",
                        "TaxStatus",
                        "TaxIsKey",
                        "CitTreatCitCount",
                        "CitBibRefCitCount",
                        "CitFigCitCount",
                        "CitTabCitCount",
                        "MatMatCitCount"],
            'delta_fields': [{'name': 'DocUploadYear',
                            'code': 'doc.uploadYear'},
                            {'name': 'DocUploadMonth',
                            'code': 'doc.uploadMonth'},
                            {'name': 'DocUpdateYear',
                            'code': 'doc.updateYear'},
                            {'name': 'DocUpdateMonth',
                            'code': 'doc.updateMonth'},
                            {'name': 'DocUploadDate',
                            'code': 'doc.uploadDate'},
                            {'name': 'DocUpdateDate',
                            'code': 'doc.updateDate'}],
            # 'params': {
            #         "load_mode": "FULL",
            #         "zone": "RAW",
            #         "project_id": "poc-plazi-raw",
            #         "dataset": "apis",
            #         "table": "tb_srsstats_treatments_stats",
            #     }
            },
        'mat_citation': {
            'query': ("https://tb.plazi.org/GgServer/srsStats/stats?"
                    "outputFields="
                        "doc.uuid+doc.articleUuid+doc.updateDate+matCit.id+matCit.country+"
                        "matCit.region+matCit.municipality+matCit.longitude+matCit.latitude+"
                        "matCit.longLatStatus+matCit.date+matCit.collector+matCit.collectionCode+"
                        "matCit.specimenCode+matCit.typeStatus"
                    "&groupingFields="
                        "doc.uuid+doc.articleUuid+doc.updateDate+matCit.id+matCit.country+"
                        "matCit.region+matCit.municipality+matCit.longitude+matCit.latitude+"
                        "matCit.longLatStatus+matCit.date+matCit.collector+matCit.collectionCode+"
                        "matCit.specimenCode+matCit.typeStatus"
                    "{delta}"
                    "&format={format}"),
            'fields': ["DocUuid",
                    "DocArticleUuid",
                    "DocUpdateDate",
                    "MatCitId",
                    "MatCitCountry",
                    "MatCitRegion",
                    "MatCitMunicipality",
                    "MatCitLongitude",
                    "MatCitLatitude",
                    "MatCitLongLatStatus",
                    "MatCitDate",
                    "MatCitCollector",
                    "MatCitCollectionCode",
                    "MatCitSpecimenCode",
                    "MatCitTypeStatus"],
            'delta_fields': [{'name': 'DocUploadYear',
                            'code': 'doc.uploadYear'},
                            {'name': 'DocUploadMonth',
                            'code': 'doc.uploadMonth'},
                            {'name': 'DocUpdateYear',
                            'code': 'doc.updateYear'},
                            {'name': 'DocUpdateMonth',
                            'code': 'doc.updateMonth'},
                            {'name': 'DocUploadDate',
                            'code': 'doc.uploadDate'},
                            {'name': 'DocUpdateDate',
                            'code': 'doc.updateDate'}],
            # 'params': {
            #         "load_mode": "FULL",
            #         "zone": "RAW",
            #         "project_id": "poc-plazi-raw",
            #         "dataset": "apis",
            #         "table": "tb_srsstats_materials_citation",
            #     }
            },
        'treat_citation': {
            'query': ("https://tb.plazi.org/GgServer/srsStats/stats?"
                    "outputFields="
                        "doc.uuid+doc.articleUuid+doc.updateDate+treatCit.citId+treatCit.rank+"
                        "treatCit.kingdomEpithet+treatCit.phylumEpithet+treatCit.classEpithet+"
                        "treatCit.orderEpithet+treatCit.familyEpithet+treatCit.genusEpithet+"
                        "treatCit.speciesEpithet+treatCit.authName+treatCit.authYear+"
                        "treatCit.citAuthor+treatCit.citYear+treatCit.citSource+"
                        "treatCit.citVolume+treatCit.citPage+treatCit.citRefString"
                    "&groupingFields="
                        "doc.uuid+doc.articleUuid+doc.updateDate+treatCit.citId+treatCit.rank+"
                        "treatCit.kingdomEpithet+treatCit.phylumEpithet+"
                        "treatCit.classEpithet+treatCit.orderEpithet+"
                        "treatCit.familyEpithet+treatCit.genusEpithet+"
                        "treatCit.speciesEpithet+treatCit.authName+treatCit.authYear+"
                        "treatCit.citAuthor+treatCit.citYear+treatCit.citSource+"
                        "treatCit.citVolume+treatCit.citPage+treatCit.citRefString"
                    "{delta}"
                    "&format={format}"),
            'fields': ["DocUuid",
                    "DocArticleUuid",
                    "DocUpdateDate",
                    "TreatCitCitId",
                    "TreatCitRank",
                    "TreatCitKingdomEpithet",
                    "TreatCitPhylumEpithet",
                    "TreatCitClassEpithet",
                    "TreatCitOrderEpithet",
                    "TreatCitFamilyEpithet",
                    "TreatCitGenusEpithet",
                    "TreatCitSpeciesEpithet",
                    "TreatCitAuthName",
                    "TreatCitAuthYear",
                    "TreatCitCitAuthor",
                    "TreatCitCitYear",
                    "TreatCitCitSource",
                    "TreatCitCitVolume",
                    "TreatCitCitPage",
                    "TreatCitCitRefString"],
            'delta_fields': [{'name': 'DocUploadYear',
                            'code': 'doc.uploadYear'},
                            {'name': 'DocUploadMonth',
                            'code': 'doc.uploadMonth'},
                            {'name': 'DocUpdateYear',
                            'code': 'doc.updateYear'},
                            {'name': 'DocUpdateMonth',
                            'code': 'doc.updateMonth'},
                            {'name': 'DocUploadDate',
                            'code': 'doc.uploadDate'},
                            {'name': 'DocUpdateDate',
                            'code': 'doc.updateDate'}],
            # 'params': {
            #         "load_mode": "FULL",
            #         "zone": "RAW",
            #         "project_id": "poc-plazi-raw",
            #         "dataset": "apis",
            #         "table": "tb_srsstats_treatment_citation",
            #     }
            }
        },
    'ephStats': {
        'basic_stats': {
            'query': ("https://tb.plazi.org/GgServer/ephStats/stats?"
                    "outputFields="
                        "doc.docId+doc.subjectDocId+doc.updateUser+"
                        "doc.updateDate+doc.updateYear+doc.updateMonth+"
                        "doc.prevDocId+doc.nextDocId+doc.errors+"
                        "doc.errorsRemoved+doc.falsePos+doc.falsePosAdded+"
                        "doc.errorsBlocker+doc.errorsRemovedBlocker+doc.falsePosBlocker+"
                        "doc.falsePosAddedBlocker+doc.errorsCritical+"
                        "doc.errorsRemovedCritical+doc.falsePosCritical+"
                        "doc.falsePosAddedCritical"
                    "&groupingFields="
                        "doc.docId+doc.subjectDocId+doc.updateUser+"
                        "doc.updateDate+doc.prevDocId+doc.nextDocId"
                    "{delta}"
                    "&format={format}"),
            'fields': ["DocDocId",
                    "DocSubjectDocId",
                    "DocUpdateUser",
                    "DocUpdateDate",
                    "DocUpdateYear",
                    "DocUpdateMonth",
                    "DocPrevDocId",
                    "DocNextDocId",
                    "DocErrors",
                    "DocErrorsRemoved",
                    "DocFalsePos",
                    "DocFalsePosAdded",
                    "DocErrorsBlocker",
                    "DocErrorsRemovedBlocker",
                    "DocFalsePosBlocker",
                    "DocFalsePosAddedBlocker",
                    "DocErrorsCritical",
                    "DocErrorsRemovedCritical",
                    "DocFalsePosCritical",
                    "DocFalsePosAddedCritical"],
            'delta_fields': [{'name': 'DocUploadYear',
                            'code': 'doc.uploadYear'},
                            {'name': 'DocUploadMonth',
                            'code': 'doc.uploadMonth'},
                            {'name': 'DocUpdateYear',
                            'code': 'doc.updateYear'},
                            {'name': 'DocUpdateMonth',
                            'code': 'doc.updateMonth'},
                            {'name': 'DocUploadDate',
                            'code': 'doc.uploadDate'},
                            {'name': 'DocUpdateDate',
                            'code': 'doc.updateDate'}],
        # 'params': {
        #             "load_mode": "FULL",
        #             "zone": "RAW",
        #             "project_id": "poc-plazi-raw",
        #             "dataset": "apis",
        #             "table": "tb_ephstats_stats",
        #         }
        }
    }
}