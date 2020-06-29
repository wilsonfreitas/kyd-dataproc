
import random

with open('../data/inf_diario_fi/csv/inf_diario_fi_202003.csv', 'rb') as fp:
    text = fp.read()

lines = text.split(b'\r\n')
sel_lines = [line for line in lines[1:] if random.random() < 0.01]

print(len(sel_lines))

new_lines = [lines[0]] + sel_lines
new_lines = [line+b'\r\n' for line in new_lines]

with open('../data/inf_diario_fi/test_inf_diario_fi_202003.csv', 'wb') as fp:
    fp.writelines(new_lines)


# import json
# from kyd.data.cvm import handle_informes_diarios


# lines = text.decode('utf_8').split('\r\n')
# data = {}
# for ix, line in enumerate(lines[1:]):
#     if line.strip():
#         key, row = handle_informes_diarios(line)
#         data_row = json.dumps(row)
#         try:
#             data[key].append(data_row)
#         except KeyError:
#             data[key] = [data_row]

# print(len(data))
