import sys
import grpc
import argparse
from concurrent import futures
import lender_pb2, lender_pb2_grpc
import pandas as pd


parser = argparse.ArgumentParser(description="argument parser for p4 clinet")


parser.add_argument("mode", help="which action to take", choices=["DbToHdfs","BlockLocations","CalcAvgLoan"])

parser.add_argument("-c", "--code", type=int, default=0, help="county code to query average loan amount in CalcAvgLoan mode")
parser.add_argument("-f", "--file", type=str, default="", help="file path for BlockLocation")
args = parser.parse_args()

channel = grpc.insecure_channel("server:5000")
stub = lender_pb2_grpc.LenderStub(channel)
if args.mode == "DbToHdfs":        
    resp = stub.DbToHdfs(lender_pb2.Empty())
    print(resp.status)
elif args.mode == "CalcAvgLoan":
    resp = stub.CalcAvgLoan(lender_pb2.CalcAvgLoanReq(county_code=args.code))
    if resp.error:
        print(f"error: {resp.error}")
    else:
        print(resp.avg_loan)
        print(resp.source)
elif args.mode == "BlockLocations":
    resp = stub.BlockLocations(lender_pb2.BlockLocationsReq(path=args.file)) 
    if resp.error:
        print(f"error: {resp.error}")
    else:
        print(resp.block_entries)




