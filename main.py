import glob, os, json, datetime as dt, math, time
import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import filedialog


class App(tk.Frame):
    def __init__(self, parent, *args, **kwargs):
        tk.Frame.__init__(self, parent, *args, **kwargs)
        self.max_width = 55
        self.file_to_process = 'File to process...'
        self.parent = parent
        self.sleep_time = 1
        #region Build Window
        self.winfo_toplevel().title("CSV Processor")

        self.top_label = tk.Label(parent,
                                  text="Select a file for processing...",
                                  font=20,
                                  width=self.max_width)
        self.top_label.grid(row=0,
                            column=0,
                            columnspan=self.max_width)
        self.select_button = tk.Button(parent,
                                       text='Browse Files',
                                       bg="AntiqueWhite1",
                                       width=10,
                                       height=2,
                                       command=self.upload_action)
        self.select_button.grid(row=2, column=(self.max_width - 15))
        # print(select_button.fileName)
        # file_to_process = select_button.Get()
        self.entry_box = tk.Label(parent,
                                  text=self.file_to_process,
                                  bg="White",
                                  width=(self.max_width),
                                  height=2,
                                  justify='left')
        self.entry_box.grid(row=2, column=5)
        #endregion

    @staticmethod
    def cleanup_output(path):
        out_files = glob.glob(path + '*')
        for f in out_files:
            os.remove(f)

    def get_latest(self):
        print('Getting latest report for processing...')
        time.sleep(self.sleep_time)
        list_of_files = glob.glob(self.input_path + '\\*')
        latest_file = max(list_of_files, key=os.path.getctime)
        vtrace_import = pd.read_csv(latest_file, parse_dates=True)
        return vtrace_import

    def get_master(self):
        print('Getting data from master file...')
        time.sleep(self.sleep_time)
        master = pd.read_csv(self.master_path)
        return master

    @staticmethod
    def get_auction_id(roi):
        if len(str(roi)) == 7:
            ret_val = int(str(roi)[:1])
        elif len(str(roi)) == 8:
            ret_val = int(str(roi)[:2])
        else:
            ret_val = int(str(roi)[:3])

        return ret_val

    def compare_master(self, new):
        vtrace = self.get_latest()
        master = self.get_master()
        print('Removing existing jobs...')
        time.sleep(self.sleep_time)
        print('Calculating Due Dates...')
        today = dt.date.today()
        vtrace_plus = vtrace.join(pd.DataFrame(
            {
                'Wheel_Dts': 0,
                'Pdr_Dts': 0,
                'Uph_Dts': 0,
                'Gls_Dts': 0,
                'Det_Dts': 0,
                'Bdy_Dts': 0,
                'Mec_Dts': 0,
                'AUCTIONID': np.nan,
                'job_spcfc_hrs': np.nan,
                'ovr_due_dt': today.strftime("%m/%d/%Y")
            }, index=vtrace.index
        ))
        vtrace_min = pd.DataFrame({}, columns=vtrace_plus.columns, index=None)

        for i, r in vtrace_plus.iterrows():
            stock_num = r['stock_nbr']
            arrival_dt = dt.datetime.strptime(r['ri_create_dtm'], '%m/%d/%Y %H:%M:%S')
            rep_item_id = r['repair_item_id']
            total_flg_hrs = 0
            total_dts = 0
            total_pm_dts = 0 
            total_bd_dts = 0 
            total_wh_dts = 0 
            total_pdr_dts = 0 
            total_uph_dts = 0
            total_glass_dts = 0 
            total_det_dts = 0
            total_mech_flg = 0
            total_bdy_flg = 0
            total_ref_flg = 0

            exists_in_master = master[(master['stock_nbr'] == stock_num) & (master['repair_item_id'] == rep_item_id)]
            stock_count_in_master = master[(master['stock_nbr'] == stock_num)]

            if exists_in_master.shape[0] == 0:
                auction_id = int(self.get_auction_id(r['repair_order_id']))
                js_pmh = int(r['parts_mech_hours']) or 0
                if 'Mech' in r['shop_type'] or js_pmh == 0:
                    js_pmh = 1
                js_rfh = int(r['refinish_hours']) or 0
                if 'Airbrush' in r['shop_type'] or (
                        'Body' in r['shop_type'] and 'Refinish' in r['insp_action']) or js_rfh == 0:
                    js_rfh = float(r['contract_amount']) * 0.01875
                js_bh = int(r['body_hours']) or 0
                if ('Body' in r['shop_type'] or 'Repair' in r['insp_action']) or js_bh == 0:
                    js_bh = float(r['contract_amount']) * 0.01875
                js_fh = (js_pmh + js_rfh + js_bh) or 0

                for k, t in stock_count_in_master.iterrows():
                    spec_job = vtrace_plus[(vtrace_plus['stock_nbr'] == t['stock_nbr'])
                                           & (vtrace_plus['repair_item_id'] == t['repair_item_id'])]
                    
                    spec_pmh = int(spec_job['parts_mech_hours']) or 0
                    if 'Mech' in spec_job['shop_type'] or spec_pmh == 0:
                        spec_pmh = 1
                    total_mech_flg = total_mech_flg + spec_pmh
                    spec_rfh = int(spec_job['refinish_hours']) or 0
                    if 'Airbrush' in spec_job['shop_type'] or ('Body' in spec_job['shop_type'] and 'Refinish' in spec_job['insp_action']) or spec_rfh == 0:
                        spec_rfh = float(spec_job['contract_amount']) * 0.01875
                    total_ref_flg = total_ref_flg + spec_rfh
                    spec_bh = int(spec_job['body_hours']) or 0
                    if ('Body' in spec_job['shop_type'] or 'Repair' in spec_job['insp_action']) or spec_bh == 0:
                        spec_bh = float(spec_job['contract_amount']) * 0.01875
                    total_bdy_flg = total_bdy_flg + spec_bh
                    spec_fh = spec_pmh + spec_rfh + spec_bh
                    spec_pm_dts = (spec_pmh / 4) or 0
                    total_pm_dts = total_pm_dts + spec_pm_dts
                    spec_bd_dts = ((spec_pmh + spec_bh) / 4) or 0
                    total_bd_dts = total_bd_dts + spec_bd_dts
                    if 'Wheel Repair' in spec_job['shop_action']:
                        spec_wh_dts = 1
                    else:
                        spec_wh_dts = 0
                    total_wh_dts = total_wh_dts + spec_wh_dts
                    if 'PDR' in spec_job['shop_action']:
                        spec_pdr_dts = 1
                    else:
                        spec_pdr_dts = 0
                    total_pdr_dts = total_pdr_dts + spec_pdr_dts
                    if 'Upholstery' in spec_job['shop_action']:
                        spec_uph_dts = 1
                    else:
                        spec_uph_dts = 0
                    total_uph_dts = total_uph_dts + spec_uph_dts
                    if 'Glass' in spec_job['shop_action']:
                        spec_glass_dts = 1
                    else:
                        spec_glass_dts = 0
                    total_glass_dts = total_glass_dts + spec_glass_dts
                    if 'Wash' in spec_job['insp_action'] or 'Complete Detail' in spec_job['insp_action']:
                        spec_det_dts = 1
                    else:
                        spec_det_dts = 0
                    total_det_dts = total_det_dts + spec_det_dts

                    spec_total_dts = spec_pm_dts + spec_bd_dts + spec_wh_dts + spec_pdr_dts + spec_uph_dts + spec_glass_dts + spec_det_dts
                    total_dts = total_dts + spec_total_dts
                    total_flg_hrs = total_flg_hrs + spec_fh

                if total_dts < 1:
                    total_dts = 1
                total_dts = math.ceil(total_dts)
                due_date = arrival_dt + dt.timedelta(days=total_dts)
                vtrace_plus.at[i, 'AUCTIONID'] = auction_id
                vtrace_plus.at[i, 'job_spcfc_hrs'] = js_fh
                vtrace_plus.at[i, 'Wheel_Dts'] = total_wh_dts
                vtrace_plus.at[i, 'Pdr_Dts'] = total_pdr_dts
                vtrace_plus.at[i, 'Uph_Dts'] = total_uph_dts
                vtrace_plus.at[i, 'Gls_Dts'] = total_glass_dts
                vtrace_plus.at[i, 'Det_Dts'] = total_det_dts
                vtrace_plus.at[i, 'Bdy_Dts'] = total_bd_dts
                vtrace_plus.at[i, 'Mec_Dts'] = total_pm_dts
                vtrace_plus.at[i, 'parts_mech_hours'] = total_mech_flg
                vtrace_plus.at[i, 'body_hours'] = total_bdy_flg
                vtrace_plus.at[i, 'refinish_hours'] = total_ref_flg
                vtrace_plus.at[i, 'ovr_due_dt'] = due_date.strftime("%m/%d/%Y")
                vtrace_min = vtrace_min.append(vtrace_plus.loc[i], ignore_index=True)
                master = master.append({
                    "stock_nbr": stock_num,
                    "repair_item_id": rep_item_id,
                    "date_added": today.strftime("%m/%d/%Y")
                }, ignore_index=True)

        return_object = {
            'new': vtrace_min.shape[0],
        }
        print("------------------")
        print(f"{vtrace_min.shape[0]} new jobs added. ")
        time.sleep(self.sleep_time)
        print("------------------")
        print("Exporting files...")
        time.sleep(self.sleep_time)
        master.to_csv(self.master_path, index=False, header=True)
        vtrace_min.to_csv(self.output_file, index=False, header=True)
        print("Cleaning up...")
        time.sleep(self.sleep_time)
        files_processed = glob.glob(self.input_path + '\\*')
        for f in files_processed:
            os.remove(f)

        return return_object

    def upload_action(self, event=None):
        filename = filedialog.askopenfilename()
        vtrace_import = pd.read_csv(filename, parse_dates=True)
        # comp_result = self.compare_master(vtrace_import)
        self.update_filename(filename)
        print('Selected:', vtrace_import.shape[0])

    def update_filename(self, text):
        self.entry_box.config(text=text)


# def main():
#     config_file_name = 'config.json'
#     if not os.path.exists(config_file_name):
#         check_create = open(config_file_name, 'w')
#         check_create.close()
#
#     try:
#         with open(config_file_name, "r") as settings_file:
#             settings_json = json.load(settings_file)
#     except:
#         user_input = input("This is your first time running this application. Please paste in the Windows Explorer path for where you have placed the ADESADorothyBase folder: ")
#         default_settings = {
#             'base_url': user_input
#         }
#         with open(config_file_name, 'w') as write_defaults:
#             json.dump(default_settings, write_defaults, ensure_ascii=False, indent=4)
#         with open(config_file_name, "r") as settings_file:
#             settings_json = json.load(settings_file)
#
#     root = tk.Tk()
#     root.geometry("500x100")
#     root.resizable(False, False)
#     a = App(root)
#
#     #region build frame
#
#     #endregion
#
#     a.mainloop()
#
#     #region old
#     # base_url = settings_json['base_url']
#     # print(f"Working Folder: {base_url}")
#     # master_path = base_url + '\\resources\\master.csv'
#     # input_path = base_url + '\\ToProcess\\'
#     # output_path = base_url + '\\Output\\'
#     # output_file = output_path + 'Vtrace_Min_'+dt.datetime.now().strftime("%m_%d_%Y_%H_%M")+'.csv'
#     #
#     # a = App(base_url, input_path, output_file, master_path)
#     # a.cleanup_output(output_path)
#     # a.compare_master()
#     # settings_file.close()
#     # print("------------------")
#     # print("Finished! You may now drop the output file into Sharepoint to be added to the Dorothy database")
#     # os.startfile(output_path)
#     # time.sleep(10)
#     # # endregion
#
#
# if __name__ == "__main__":
#     main()

def main():
    root = tk.Tk()
    root.geometry("500x100")
    root.resizable(False, False)
    a = App(root)
    root.mainloop()


# runs main function
if __name__ == "__main__":
    main()
