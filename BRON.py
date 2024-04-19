#!/usr/bin/env python3

import sys

import optparse

# helper functions that are shared
import raidpirlib as lib

# used to issue requests in parallel
import threading

import simplexorrequestor

import session

# for basename
import os.path

# to sleep...
import time
_timer = lib._timer


def _request_helper(rxgobj, tid):
	"""Private helper to get requests.
	Multiple threads will execute this, each with a unique tid."""
	thisrequest = rxgobj.get_next_xorrequest(tid)

	#the socket is fixed for each thread, so we only need to do this once
	socket = thisrequest[0]['sock']

	# go until there are no more requests
	while thisrequest != ():
		bitstring = thisrequest[2]
		try:
			# request the XOR block...
			lib.request_xorblock(socket, bitstring)

		except Exception as e:
			if 'socked' in str(e):
				rxgobj.notify_failure(thisrequest)
				sys.stdout.write('F')
				sys.stdout.flush()
			else:
				# otherwise, re-raise...
				raise

		# regardless of failure or success, get another request...
		thisrequest = rxgobj.get_next_xorrequest(tid)

	# and that's it!
	return


def _request_helper_chunked(rxgobj, tid):
	"""Private helper to get requests with chunks.
	Potentially multiple threads will execute this, each with a unique tid."""
	thisrequest = rxgobj.get_next_xorrequest(tid)

	#the socket is fixed for each thread, so we only need to do this once
	socket = thisrequest[0]['sock']
	rqtype = thisrequest[3] #the request type is also fixed

	# go until there are no more requests
	while thisrequest != ():
		chunks = thisrequest[2]

		try:
			# request the XOR block...
			if rqtype == 1: # chunks and seed expansion
				lib.request_xorblock_chunked_rng(socket, chunks)

			elif rqtype == 2: # chunks, seed expansion and parallel
				lib.request_xorblock_chunked_rng_parallel(socket, chunks)

			else: # only chunks (redundancy)
				lib.request_xorblock_chunked(socket, chunks)

		except Exception as e:
			if 'socked' in str(e):
				rxgobj.notify_failure(thisrequest)
				sys.stdout.write('F')
				sys.stdout.flush()
			else:
				# otherwise, re-raise...
				raise

		# regardless of failure or success, get another request...
		thisrequest = rxgobj.get_next_xorrequest(tid)

	# and that's it!
	return


def request_blocks_from_mirrors(requestedblocklist, manifestdict, redundancy, rng, parallel):
	"""
	<Purpose>
		Retrieves blocks from mirrors

	<Arguments>
		requestedblocklist: the blocks to acquire

		manifestdict: the manifest with information about the release

	<Side Effects>
		Contacts mirrors to retrieve blocks. It uses some global options

	<Exceptions>
		TypeError may be raised if the provided lists are invalid.
		socket errors may be raised if communications fail.

	<Returns>
		A dict mapping blocknumber -> blockcontents.
	"""

	# let's get the list of mirrors...
	if _commandlineoptions.vendorip == None:
		# use data from manifest
		mirrorinfolist = lib.retrieve_mirrorinfolist(manifestdict['vendorhostname'], manifestdict['vendorport'])
	else:
		# use commandlineoption
		mirrorinfolist = lib.retrieve_mirrorinfolist(_commandlineoptions.vendorip)

	print("Mirrors: ", mirrorinfolist)

	if _commandlineoptions.timing:
		setup_start = _timer()

	# no chunks (regular upPIR / Chor)
	if redundancy == None:

		# let's set up a requestor object...
		rxgobj = simplexorrequestor.RandomXORRequestor(mirrorinfolist, requestedblocklist, manifestdict, _commandlineoptions.numberofmirrors, _commandlineoptions.batch, _commandlineoptions.timing)

		if _commandlineoptions.timing:
			setup_time = _timer() - setup_start
			_timing_log.write(str(len(rxgobj.activemirrors[0]['blockbitstringlist']))+"\n")
			_timing_log.write(str(len(rxgobj.activemirrors[0]['blockbitstringlist']))+"\n")

		print("Blocks to request:", len(rxgobj.activemirrors[0]['blockbitstringlist']))

		if _commandlineoptions.timing:
			req_start = _timer()

		# let's fire up the requested number of threads.   Our thread will also participate (-1 because of us!)
		for tid in range(_commandlineoptions.numberofmirrors - 1):
			threading.Thread(target=_request_helper, args=[rxgobj, tid]).start()

		_request_helper(rxgobj, _commandlineoptions.numberofmirrors - 1)

		# wait for receiving threads to finish
		for mirror in rxgobj.activemirrors:
			mirror['rt'].join()

	else: # chunks

		# let's set up a chunk requestor object...
		rxgobj = simplexorrequestor.RandomXORRequestorChunks(mirrorinfolist, requestedblocklist, manifestdict, _commandlineoptions.numberofmirrors, redundancy, rng, parallel, _commandlineoptions.batch, _commandlineoptions.timing)

		if _commandlineoptions.timing:
			setup_time = _timer() - setup_start
			_timing_log.write(str(len(rxgobj.activemirrors[0]['blocksneeded']))+"\n")
			_timing_log.write(str(len(rxgobj.activemirrors[0]['blockchunklist']))+"\n")

		print("# Blocks needed:", len(rxgobj.activemirrors[0]['blocksneeded']))

		if parallel:
			print("# Requests:", len(rxgobj.activemirrors[0]['blockchunklist']))

		#chunk lengths in BYTE
		global chunklen
		global lastchunklen
		chunklen = (manifestdict['blockcount'] / 8) / _commandlineoptions.numberofmirrors
		lastchunklen = lib.bits_to_bytes(manifestdict['blockcount']) - (_commandlineoptions.numberofmirrors-1)*chunklen

		if _commandlineoptions.timing:
			req_start = _timer()

		# let's fire up the requested number of threads.   Our thread will also participate (-1 because of us!)
		for tid in range(_commandlineoptions.numberofmirrors - 1):
			threading.Thread(target=_request_helper_chunked, args=[rxgobj, tid]).start()

		_request_helper_chunked(rxgobj, _commandlineoptions.numberofmirrors - 1)

		# wait for receiving threads to finish
		for mirror in rxgobj.activemirrors:
			mirror['rt'].join()

	rxgobj.cleanup()

	if _commandlineoptions.timing:
		req_time = _timer() - req_start
		recons_time, comptimes, pings = rxgobj.return_timings()

		avg_ping = sum(pings) / _commandlineoptions.numberofmirrors
		avg_comptime = sum(comptimes) / _commandlineoptions.numberofmirrors

		_timing_log.write(str(setup_time)+ "\n")
		_timing_log.write(str(req_time)+ "\n")
		_timing_log.write(str(recons_time)+ "\n")
		_timing_log.write(str(avg_comptime)+ " " + str(comptimes)+ "\n")
		_timing_log.write(str(avg_ping)+ " " + str(pings)+ "\n")

	# okay, now we have them all. Let's get the returned dict ready.
	retdict = {}
	for blocknum in requestedblocklist:
		retdict[blocknum] = rxgobj.return_block(blocknum)

	return retdict


def request_files_from_mirrors(requestedfilelist, redundancy, rng, parallel, manifestdict):
	"""
	<Purpose>
		Reconstitutes files by privately contacting mirrors

	<Arguments>
		requestedfilelist: the files to acquire
		redundancy: use chunks and overlap this often
		rng: use rnd to generate latter chunks
		parallel: query one block per chunk
		manifestdict: the manifest with information about the release

	<Side Effects>
		Contacts mirrors to retrieve files. They are written to disk

	<Exceptions>
		TypeError may be raised if the provided lists are invalid.
		socket errors may be raised if communications fail.

	<Returns>
		None
	"""

	neededblocks = []
	#print "Request Files:"
	# let's figure out what blocks we need
	for filename in requestedfilelist:
		theseblocks = lib.get_blocklist_for_file(filename, manifestdict)

		# add the blocks we don't already know we need to request
		for blocknum in theseblocks:
			if blocknum not in neededblocks:
				neededblocks.append(blocknum)

	# do the actual retrieval work
	blockdict = request_blocks_from_mirrors(neededblocks, manifestdict, redundancy, rng, parallel)

	# now we should write out the files
	for filename in requestedfilelist:
		filedata = lib.extract_file_from_blockdict(filename, manifestdict, blockdict)

		# let's check the hash
		thisfilehash = lib.find_hash(filedata, manifestdict['hashalgorithm'])

		for fileinfo in manifestdict['fileinfolist']:
			# find this entry
			if fileinfo['filename'] == filename:
				if thisfilehash == fileinfo['hash']:
					# we found it and it checks out!
					break
				else:
					raise Exception("Corrupt manifest has incorrect file hash despite passing block hash checks!")
		else:
			raise Exception("Internal Error: Cannot locate fileinfo in manifest!")


		# open the filename w/o the dir and write it
		filenamewithoutpath = os.path.basename(filename)
		open(filenamewithoutpath, "wb").write(filedata)
		print("wrote", filenamewithoutpath)


########################## Option parsing and main ###########################
_commandlineoptions = None

def parse_options():
	"""
	<Purpose>
		Parses command line arguments.

	<Arguments>
		None

	<Side Effects>
		All relevant data is added to _commandlineoptions

	<Exceptions>
		These are handled by optparse internally.   I believe it will print / exit
		itself without raising exceptions further.   I do print an error and
		exit if there are extra args...

	<Returns>
		The list of files to retrieve
	"""
	global _commandlineoptions

	# should be true unless we're initing twice...
	assert _commandlineoptions == None

	parser = optparse.OptionParser()

	parser.add_option("", "--retrievemanifestfrom", dest="retrievemanifestfrom",
				type="string", metavar="vendorIP:port", default="",
				help="Specifies the vendor to retrieve the manifest from (default None).")

	parser.add_option("", "--printfilenames", dest="printfiles",
				action="store_true", default=False,
				help="Print a list of all available files in the manifest file.")

	parser.add_option("", "--vendorip", dest="vendorip", type="string", metavar="IP",
				default=None, help="Vendor IP for overwriting the value from manifest; for testing purposes.")

	parser.add_option("-m", "--manifestfile", dest="manifestfilename",
				type="string", default="manifest.dat",
				help="The manifest file to use (default manifest.dat).")

	parser.add_option("-k", "--numberofmirrors", dest="numberofmirrors",
				type="int", default=2,
				help="How many servers do we query? (default 2)")

	parser.add_option("-r", "--redundancy", dest="redundancy",
				type="int", default=None,
				help="Activates chunks and specifies redundancy r (how often they overlap). (default None)")

	parser.add_option("-R", "--rng", action="store_true", dest="rng", default=False,
				help="Use seed expansion from RNG for latter chunks (default False). Requires -r")

	parser.add_option("-p", "--parallel", action="store_true", dest="parallel", default=False,
				help="Query one block per chunk in parallel (default False). Requires -r")

	parser.add_option("-b", "--batch", action="store_true", dest="batch", default=False,
				help="Request the mirror to do computations in a batch. (default False)")

	parser.add_option("-t", "--timing", action="store_true", dest="timing", default=False,
				help="Do timing measurements and print them at the end. (default False)")

	parser.add_option("-c", "--comment", type="string", dest="comment", default="",
				help="Debug comment on this run, used to name timing log file.")


	# let's parse the args
	(_commandlineoptions, remainingargs) = parser.parse_args()

	# Force the use of a seeded rng (-R) if MB (-p) is used. Temporary, until -p without -R is implemented.
	if _commandlineoptions.parallel:
		_commandlineoptions.rng = True

	# sanity check parameters

	# k >= 2
	if _commandlineoptions.numberofmirrors < 2:
		print("Mirrors to contact must be > 1")
		sys.exit(1)

	# r >= 2
	if _commandlineoptions.redundancy != None and _commandlineoptions.redundancy < 2:
		print("Redundancy must be > 1")
		sys.exit(1)

	# r <= k
	if _commandlineoptions.redundancy != None and (_commandlineoptions.redundancy > _commandlineoptions.numberofmirrors):
		print("Redundancy must be less or equal to number of mirrors (", _commandlineoptions.numberofmirrors, ")")
		sys.exit(1)

	# RNG or parallel query without chunks activated
	if (_commandlineoptions.rng or _commandlineoptions.parallel) and not _commandlineoptions.redundancy:
		print("Chunks must be enabled and redundancy set (-r <number>) to use RNG or parallel queries!")
		sys.exit(1)

	if len(remainingargs) == 0 and _commandlineoptions.printfiles == False:
		print("Must specify at least one file to retrieve!")
		sys.exit(1)

	#filename(s)
	_commandlineoptions.filestoretrieve = remainingargs


def start_logging():
		global _timing_log
		global total_start

		logfilename = time.strftime("%y%m%d") + "_" + _commandlineoptions.comment
		logfilename += "_k" + str(_commandlineoptions.numberofmirrors)
		if _commandlineoptions.redundancy:
			logfilename += "_r" + str(_commandlineoptions.redundancy)
		if _commandlineoptions.rng:
			logfilename += "_R"
		if _commandlineoptions.parallel:
			logfilename += "_p"
		if _commandlineoptions.batch:
			logfilename += "_b"

		cur_time = time.strftime("%y%m%d-%H%M%S")
		_timing_log = open("timing_" + logfilename + ".log", "a")
		_timing_log.write(cur_time + "\n")
		_timing_log.write(str(_commandlineoptions.filestoretrieve) + " ")
		_timing_log.write(str(_commandlineoptions.numberofmirrors) + " ")
		_timing_log.write(str(_commandlineoptions.redundancy) + " ")
		_timing_log.write(str(_commandlineoptions.rng) + " ")
		_timing_log.write(str(_commandlineoptions.parallel) + " ")
		_timing_log.write(str(_commandlineoptions.batch) + "\n")


		total_start = _timer()



def main():
	"""main function with high level control flow"""

	# If we were asked to retrieve the mainfest file, do so...
	if _commandlineoptions.retrievemanifestfrom:
		# We need to download this file...
		rawmanifestdata = lib.retrieve_rawmanifest(_commandlineoptions.retrievemanifestfrom)

		# ...make sure it is valid...
		manifestdict = lib.parse_manifest(rawmanifestdata)

		# ...and write it out if it's okay
		open(_commandlineoptions.manifestfilename, "wb").write(rawmanifestdata)

	else:
		# Simply read it in from disk
		rawmanifestdata = open(_commandlineoptions.manifestfilename, "rb").read()

		manifestdict = lib.parse_manifest(rawmanifestdata)

	# we will check that the files are in the release

	# find the list of files
	filelist = lib.get_filenames_in_release(manifestdict)

	if (manifestdict['blockcount'] < _commandlineoptions.numberofmirrors * 8) and _commandlineoptions.redundancy != None:
		print("Block count too low to use chunks! Try reducing the block size or add more files to the database.")
		sys.exit(1)

	if _commandlineoptions.printfiles:
		print("Manifest - Blocks:", manifestdict['blockcount'], "x", manifestdict['blocksize'], "Byte - Files:\n", filelist)

	if _commandlineoptions.timing:
		_timing_log.write(str(manifestdict['blocksize']) + "\n")
		_timing_log.write(str(manifestdict['blockcount']) + "\n")

	# ensure the requested files are in there...
	for filename in _commandlineoptions.filestoretrieve:

		if filename not in filelist:
			print("The file", filename, "is not listed in the manifest.")
			sys.exit(2)

	# don't run PIR if we're just printing the filenames in the manifest
	if len(_commandlineoptions.filestoretrieve) > 0:
		request_files_from_mirrors(_commandlineoptions.filestoretrieve, _commandlineoptions.redundancy, _commandlineoptions.rng, _commandlineoptions.parallel, manifestdict)

if __name__ == '__main__':
	print("RAID-PIR Client", lib.pirversion)
	parse_options()

	if _commandlineoptions.timing:
		start_logging()

		import numpy as np
import hashlib
import random

class PIRDL:
    def __init__(self, num_blocks, block_size, database):
        self.num_blocks = num_blocks
        self.block_size = block_size
        self.database = database
        self.H = lambda x: hashlib.sha256(x.encode()).hexdigest()

    def generate_query(self, index):
        query = np.zeros((self.num_blocks, self.block_size), dtype=int)
        query[index] = 1
        return query

    def process_response(self, response):
        decoded_response = []
        for block in response:
            decoded_block = ''
            for bit in block:
                if bit == 1:
                    decoded_block += '1'
                else:
                    decoded_block += '0'
            decoded_response.append(int(decoded_block, 2))
        return decoded_response

    def execute_query(self, index):
        query = self.generate_query(index)
        response = []
        for block_index in range(self.num_blocks):
            encoded_block = ''.join(map(str, query[block_index]))
            hashed_block = self.H(encoded_block)
            response_block = self.database[hashed_block]
            response.append(response_block)
        return response

    def registration(self, O_ID, a, b):
        K_plus = self.H(O_ID)
        r = random.randint(1, self.num_blocks)
        K_minus = r * K_plus
        consent = 1
        self.update_blockchain(O_ID, K_minus)
        return K_plus, K_minus

    def update_blockchain(self, O_ID, K_minus):
        blockchain_entry = f"{O_ID}:{K_minus}"
        self.database[self.H(blockchain_entry)] = blockchain_entry

    def credential_verification(self, v_req):
        verifier, issuance, expiry, sign_verifier = v_req
        if self.consents[verifier] == 1:
            if sign_verifier == True:
                if self.check_expiry(expiry) and self.check_issuance(issuance):
                    return True, self.timestamp
        return False, self.timestamp

    def check_expiry(self, expiry):
        # Implement expiry check
        return True

    def check_issuance(self, issuance):
        # Implement issuance check
        return True

    def poa(self, v_req, V):
        C = self.credential_verification(v_req)
        B = self.initialize_block()
        self.broadcast_block(B, self.sign_K_plus(V))
        while True:
            self.check_timestamp()
            self.validate_sign_K_plus()
            if self.consents[V] == 1:
                self.check_anonymity()
                self.check_unlinkability()
                self.add_block_to_blockchain(B)
                self.consensus = True
                self.rep += 1
                return True
            else:
                self.consensus = False
                return False

    def initialize_block(self):
        return f"Block{random.randint(1, 100)}"

    def broadcast_block(self, B, signature):
        # Implement broadcasting
        pass

    def check_timestamp(self):
        # Implement timestamp check
        pass

    def validate_sign_K_plus(self):
        # Implement signature validation
        pass

    def check_anonymity(self):
        # Implement anonymity check
        pass

    def check_unlinkability(self):
        # Implement unlinkability check
        pass

    def add_block_to_blockchain(self, B):
        # Implement adding block to blockchain
        pass

    def smart_contract(self, KPIs, EMP_ID, EMP_ID_time):
        KPI_min = min(KPIs)
        while EMP_ID_time:
            KPI_EMP = sum(KPIs[EMP_ID])
            if KPI_EMP >= KPI_min:
                self.incentivize(EMP_ID)
                # Contract extension/promotion
            else:
                self.disincentivize(EMP_ID)

    def incentivize(self, EMP_ID):
        # Implement incentivization
        pass

    def disincentivize(self, EMP_ID):
        # Implement disincentivization
        pass

    def pir(self, B, Q, epsilon, delta, Psi, ZKP):
        if self.verify_signature(owner):
            self.allow_RADP(Requester)
        else:
            if self.consents[Requester] == 1:
                self.select_privacy_mechanism(ZKP)
                rho = 0  # Initialize noise parameter
                Psi_cumulative = 0
                for q_i in range(1, j):
                    if Psi_cumulative + epsilon > Psi:
                        break
                    else:
                        Psi_cumulative += epsilon
                        w = self.generate_witness()
                        P = self.transform_compute_constraints(w)
                        sigma_c = self.commit_to_polynomials(polynomials)
                        proof = self.create_proof(sigma_c)
                        proof_received = self.get_proof_from_prover()
                        result_ver = self.verify_proof(key_ver, proof_received)
                        if result_ver:
                            self.accept_computation()
                            R_i = self.compute_noisy_response()
                            return R_i
                        else:
                            self.reject_computation()
                else:
                    self.alert("Consent NULL, Information retrieval failed")


	main()

	if _commandlineoptions.timing:
		ttime = _timer() - total_start
		_timing_log.write(str(ttime)+ "\n")
		_timing_log.close()
