
import argparse
import os
import io
import pypdf
from typing import Any, Callable, Dict, Tuple, Union, cast
from pypdf.generic import DictionaryObject, NameObject, RectangleObject, ContentStream, ArrayObject
from pypdf.generic._base import TextStringObject, ByteStringObject, NumberObject, FloatObject
from pypdf.constants import PageAttributes as PG
from pypdf._cmap import build_char_map


class CharMap:
    def __init__(self, subtype, halfspace, encoding, map, ft):
        [setattr(self, k, v) for k, v in locals().items()]

    @classmethod
    def from_char_map(cls, subtype: str, halfspace: float, encoding: Union[str, Dict[int, str]], map: Dict[str, str],
                      ft: DictionaryObject):
        return cls(subtype, halfspace, encoding, map, ft)

    def decode(self, text: Union[TextStringObject, ByteStringObject]):
        
        
        if (isinstance(text, TextStringObject) and self.encoding == "charmap"):
            
            b=text.get_original_bytes()
            temp="".join(text.get_original_bytes().decode('latin1').translate(str.maketrans(self.map)))
            return temp
        elif (isinstance(text, ByteStringObject)):
            return "".join(text.decode(self.encoding).translate(str.maketrans(self.map)))
        elif (isinstance(self.encoding, dict)):
            return str(text)  
        else:
            
            
            
            return ""
            
            

    def encode(self, text, reference):
        
        if (isinstance(self.encoding, dict)):
            t = TextStringObject(text)
            t.autodetect_pdfdocencoding = reference.autodetect_pdfdocencoding
            t.autodetect_utf16 = reference.autodetect_utf16
            return t
        elif (self.encoding == "charmap"):
            map = {v: k for k, v in self.map.items()}
            return ByteStringObject(
                text.translate(str.maketrans(map)).encode('latin1'))  
        elif (isinstance(reference, ByteStringObject)):
            map = {v: k for k, v in self.map.items() if not isinstance(v, str) or len(v) == 1}
            return ByteStringObject(text.translate(str.maketrans(map)).encode(self.encoding))
        else:
            raise NotImplementedError(f"Cannot encode this {type(self.encoding)} encoding: {self.encoding}")



def get_char_maps(obj: Any, space_width: float = 200.0) -> Dict[str, CharMap]:
    cmaps = {}
    objr = obj
    while NameObject(PG.RESOURCES) not in objr:
        
        objr = objr["/Parent"].get_object()
    resources_dict = cast(DictionaryObject, objr[PG.RESOURCES])
    if "/Font" in resources_dict:
        for font_id in cast(DictionaryObject, resources_dict["/Font"]):
            cmaps[font_id] = CharMap.from_char_map(*build_char_map(font_id, space_width, obj))
    return cmaps


class MappedOperand:
    def __init__(self, operation, operand, text):
        [setattr(self, k, v) for k, v in locals().items()]

    def __repr__(self):
        return f"{str(self.operation.operator)} → „{self.text}“"


class Context:
    def __init__(self, font: str = None):
        self.font = font

    def clone(self):
        return type(self)(self.font)


class PDFOperation:
    def __init__(self, operands, operator, context: Context):
        self.operands = operands
        self.operator = operator
        self.context = context

    @classmethod
    def from_tuple(cls, operands, operator, context: Context):
        
        
        operator = operator.decode('latin1')  
        classname = f"PDFOperation{operator}"
        if (classname in globals()):
            return globals()[classname](operands, context)
        return cls(operands, operator, None)

    def __repr__(self):
        return self.operator

    def write_to_stream(self, stream):
        if isinstance(self.operands, dict):
            return
        
        
        for op in self.operands:
            
            op.write_to_stream(stream)

            
            
            
            
            stream.write(b" ")
        stream.write(self.operator.encode("latin1"))  
        stream.write(b"\n")

    def get_text_map(self, charmaps):
        return [MappedOperand(self, None, None)]


class PDFOperationTf(PDFOperation):
    def __init__(self, operands, context: Context):
        super().__init__(operands, "Tf", None)
        context.font = operands[0]


class PDFOperationTd(PDFOperation):
    def __init__(self, operands, context: Context):
        super().__init__(operands, "Td", None)

    def __repr__(self):
        return f"{self.operands} {self.operator}"

    def get_text_map(self, charmaps):
        map = []
        tx, ty = self.operands
        if (ty != 0):
            
            map.append(MappedOperand(self, None, "\n"))
        elif (tx != 0):
            
            map.append(MappedOperand(self, None, " "))
        return map


class PDFOperationTJ(PDFOperation):
    def __init__(self, operands: list[list[Union[TextStringObject, ByteStringObject, NumberObject]]], context: Context):
        if (len(operands) != 1):
            raise ValueError(f"PDFOperationTJ expects one non-empty Array of Array")
        super().__init__(operands, "TJ", context.clone())

    def __repr__(self):
        return f"„{self.operands[0]}“ {self.operator}"

    def get_text_map(self, charmaps):
        map = []
        for operand in self.operands[0]:
            if (isinstance(operand, NumberObject) or isinstance(operand, FloatObject)):
                if (operand < -charmaps[self.context.font].halfspace):
                    
                    map.append(MappedOperand(self, operand, " "))
            else:
                map.append(MappedOperand(self, operand, charmaps[self.context.font].decode(operand)))
        return map

    def replace_text(self, text, charmaps, start, end):
        
        pre = []
        if (start is not None):
            start = self.operands[0].index(start)
            pre = self.operands[0][:start]
        
        post = []
        if (end is not None):
            end = self.operands[0].index(end)
            post = self.operands[0][end + 1:]
        mid = []
        if (text):
            sample = next(
                (op for op in self.operands[0] if isinstance(op, TextStringObject) or isinstance(op, ByteStringObject)))
            mid = [charmaps[self.context.font].encode(text, sample)]
        self.operands[0] = ArrayObject(pre + mid + post)


class PDFOperationTj(PDFOperation):
    def __init__(self, operands: list[Union[TextStringObject, ByteStringObject]], context: Context):
        if (len(operands) != 1):
            raise ValueError(f"PDFOperationTj expects one non-empty Array of TextStringObject")
        super().__init__(operands, "Tj", context.clone())

    def __repr__(self):
        return f"„{self.operands[0]}“ {self.operator}"

    def get_text_map(self, charmaps):
        return [MappedOperand(self, self.operands[0], charmaps[self.context.font].decode(self.operands[0]))]

    def replace_text(self, text, charmaps, start, end):
        self.operands[0] = charmaps[self.context.font].encode(text, self.operands[0])


def search_in_mappings(text_maps, needle):
    
    
    needle_index = 0
    start = None
    end = None
    for operation_level in text_maps:  
        for mapped_operand in operation_level:
            if (mapped_operand.text):
                mapped_operand.text = mapped_operand.text.lower()
                
                for haystack_index, c in enumerate(mapped_operand.text):
                    if (c == needle[needle_index].lower()):
                        
                        if (needle_index == 0):
                            start = MappedOperand(mapped_operand.operation, mapped_operand.operand,
                                                  mapped_operand.text[:haystack_index])
                        needle_index += 1
                        if (needle_index == len(needle)):
                            end = MappedOperand(mapped_operand.operation, mapped_operand.operand,
                                                mapped_operand.text[haystack_index + 1:])
                            break
                    else:
                        needle_index = 0
            if (needle_index == len(needle)):
                break
        if (needle_index == len(needle)):
            break
    return (start, end)


def replace_operations(operations: list[PDFOperation], start: MappedOperand, end: MappedOperand, replacement, charmaps):
    out = []
    tds = []
    keep = True
    for operation in operations:
        if (keep):
            out.append(operation)
        elif (operation.operator == "Td"):
            
            
            tds.append(operation)
        if (operation == start.operation):
            
            keep = False
            replacement = start.text + replacement
            if (operation == end.operation):
                
                replacement += end.text
                keep = True
            end_operand = end.operand
            if (operation != end.operation):
                end_operand = None
            operation.replace_text(replacement, charmaps, start.operand, end_operand)
            if (operation == end.operation):
                out.extend(tds)
        if (operation == end.operation and start.operation != end.operation):
            
            replacement = end.text  
            operation.replace_text(replacement, charmaps, None, end.operand)
            out.append(operation)
            out.extend(tds)
            keep = True
    return out


def replace_text(content: ContentStream, charmaps: Dict[str, CharMap], needle: str, replacement: str) -> int:
    replacement_count = 0
    
    
    context = Context()
    temp_test=content.operations
    operations = [PDFOperation.from_tuple(ops, op, context) for ops, op in content.operations]
    
    
    while (True):
        text_maps = [op.get_text_map(charmaps) for op in operations]
        
        if (needle is None or replacement is None):
            print("".join(["".join([t.text for t in text_map if t.text]) for text_map in text_maps]))
            break
        else:
            start, end = search_in_mappings(text_maps, needle)
            
            
            if (start and end):
                operations = replace_operations(operations, start, end, replacement, charmaps)
                replacement_count += 1
            else:
                break
    
    
    stream = io.BytesIO()
    [op.write_to_stream(stream) for op in operations]
    
    content.set_data(stream.getvalue())
    return replacement_count


def pypdf_replace(file_path: str, replace_json: dict):
    try:
        result_path = None
        for key, value in replace_json.items():
            if os.path.exists(file_path + ".pdf"):
                args_input = file_path + ".pdf"
            else:
                args_input = file_path
            args_output = file_path + ".pdf"
            args_search = key
            args_replace = value
            args_papersize = ""
            if (args_search is None or args_replace is None):
                print("These are the lines this tool might be able to handle:")

            total_replacements = 0
            file = open(file=args_input, mode='rb+')
            reader = pypdf.PdfReader(file)
            writer = pypdf.PdfWriter()

            for page_index, page in enumerate(reader.pages):
                
                charmaps = get_char_maps(page)
                contents = page.get_contents()
                text = page.extract_text()
                
                if (isinstance(contents, pypdf.generic._data_structures.ArrayObject)):
                    for content in contents:
                        total_replacements += replace_text(content, charmaps, args_search, args_replace)
                elif (isinstance(contents, pypdf.generic._data_structures.ContentStream)):
                    total_replacements += replace_text(contents, charmaps, args_search, args_replace)
                else:
                    
                    pass
                page.replace_contents(contents)

                if (args_papersize):
                    papersize = getattr(pypdf.PaperSize, args_papersize)
                    page.mediabox = RectangleObject((0, 0, papersize.width, papersize.height))
                writer.add_page(page)

            print(f"Replaced {total_replacements} occurrences.")

            if (args_output):
                
                
                writer.write(args_output)
                result_path = args_output
    except Exception as e:
        result_path = f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
    return result_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Replace text in a PDF file.')
    parser.add_argument('--input', type=str, required=True)
    parser.add_argument('--output', type=str)
    parser.add_argument('--search', type=str)
    parser.add_argument('--replace', type=str)
    parser.add_argument('--papersize', type=str)
    args = parser.parse_args()

    if (args.search is None or args.replace is None):
        print("These are the lines this tool might be able to handle:")

    total_replacements = 0
    reader = pypdf.PdfReader(args.input)
    writer = pypdf.PdfWriter()

    for page_index, page in enumerate(reader.pages):
        

        charmaps = get_char_maps(page)

        contents = page.get_contents()
        
        if (isinstance(contents, pypdf.generic._data_structures.ArrayObject)):
            for content in contents:
                total_replacements += replace_text(content, charmaps, args.search, args.replace)
        elif (isinstance(contents, pypdf.generic._data_structures.ContentStream)):
            total_replacements += replace_text(contents, charmaps, args.search, args.replace)
        else:
            raise NotImplementedError(f"Cannot modify {type(contents)}.")
        page.replace_contents(contents)

        if (args.papersize):
            papersize = getattr(pypdf.PaperSize, args.papersize)
            page.mediabox = RectangleObject((0, 0, papersize.width, papersize.height))
        writer.add_page(page)

    print(f"Replaced {total_replacements} occurrences.")

    if (args.output):
        writer.write(args.output)
