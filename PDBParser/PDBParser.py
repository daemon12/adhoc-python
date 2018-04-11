#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 10 20:26:08 2018
@author: pradeep

The code parses the PDB file from RCSB and returns the data in a data structure
to use it further in the analysis  
"""
from collections import defaultdict

class PDBParser(object):
    """
        Class to parse PDB file and retrieve basic information
    """
    def __init__(self, pdbfile):
        self._pdbf = pdbfile
        self.atom_data_keys = ["NAME", "RESNAME", "CHAIN", "RESNUM", "X", "Y", "Z"]
        self.atom_data = dict()
        self.res_data_keys = ["RESNAME", "ATOMS"]
        self.res_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        self.parsePDB()

    def parsePDB(self):
        pdbFH = open(self._pdbf, 'r')
        for line in pdbFH:
            if line.startswith("ATOM"):
                #Split the line into datat points
                atom_num   = line[6:11].strip()
                atom_name  = line[11:17].strip()
                res_name   = line[17:21].strip()
                chainID    = line[21:22].strip()
                res_num    = line[22:28].strip()
                x_cord     = line[28:38].strip()
                y_cord     = line[38:46].strip()
                z_cord     = line[46:54].strip()

                #Put atom data into dictionary {KEY=ATOM_NUM, VALUE=atom_data_keys}
                self.atom_data[atom_num]= [atom_name, res_name, 
                              chainID, res_num, x_cord, y_cord, z_cord]

                #Put residue data into dictionary  {KEY=CHAIN_RESNUM, VALUE=DETAILS}
                self.res_data[chainID][res_num]['RESNAME'] = res_name
                self.res_data[chainID][res_num]['ATOMS'].append(atom_num)

    #ATOM related data GETTERS
    def get_atom_details(self, atomnum):
        return self.atom_data[atomnum]

    def get_atom_name(self, atomnum):
        return self.atom_data[atomnum][0]

    def get_atom_resname(self, atomnum):
        return self.atom_data[atomnum][1]

    def get_atom_chain(self, atomnum):
        return self.atom_data[atomnum][2]

    def get_atom_resnum(self, atomnum):
        return self.atom_data[atomnum][3]

    def get_atom_cord(self, atomnum):
        return self.atom_data[atomnum][4:]

    #RESIDUE related data GETTERS
    def get_res_details(self, chain, resnum):
        return self.res_data[chain][resnum]

    def get_res_name(self, chain, resnum):
        return self.res_data[chain][resnum]['RESNAME']

    def get_res_atoms(self, chain, resnum):
        return self.res_data[chain][resnum]['ATOMS']
 
        
##############################################
##Client code where data is being retrieved###
##############################################
if __name__ == '__main__':
    parser = PDBParser('/home/pradeep/Documents/F1/Gulzar/1z5s.pdb')
    print zip(parser.atom_data_keys, parser.get_atom_details('45'))
    print "ALL atom details:" + str(parser.get_atom_details('2054'))
    print "atom name:" + parser.get_atom_name('2054')
    print "atom resname:" + parser.get_atom_resname('2054')
    print "atom chain:" + parser.get_atom_chain('2054')
    print "atom resnum:" + parser.get_atom_resnum('2054')
    print "atom coord:" + str(parser.get_atom_cord('2054'))
    print "ALL residue details:" + str(parser.get_res_details('D', '2672'))
    print "residue name:" + str(parser.get_res_name('D', '2672'))
    print "residue atoms:" + str(parser.get_res_atoms('D', '2672'))

